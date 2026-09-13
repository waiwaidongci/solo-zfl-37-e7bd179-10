import http from "node:http";
import { mkdir, readFile, writeFile, rename, unlink } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = process.env.DB_PATH
  ? process.env.DB_PATH
  : join(__dirname, "data", "ink-stick-testing.json");
const port = Number(process.env.PORT || 3037);

const SCHEMA_VERSION = 2;

const seed = {
  "items": [
    {
      "code": "IS-001",
      "smokeSource": "黄山松烟",
      "glueRatio": "7.5%",
      "ageYears": 8,
      "storage": "恒湿柜B",
      "status": "已试磨",
      "logs": [
        {
          "at": "2026-06-11",
          "step": "试磨",
          "note": "宣纸20滴水，出墨快，评分86",
          "score": 86
        }
      ]
    },
    {
      "code": "IS-002",
      "smokeSource": "桐油烟",
      "glueRatio": "8%",
      "ageYears": 3,
      "storage": "试样盒C",
      "status": "待试磨",
      "logs": []
    }
  ]
};

/* ---------------- 风险追溯域常量 ---------------- */

// 轻量账号体系（演示/越权验证用）：头部 x-user-id 标识操作人
const users = [
  { id: "u01", name: "沈研工", role: "检验员" },
  { id: "u02", name: "林质管", role: "质量主管" },
  { id: "u03", name: "周复核", role: "复核员" }
];
const userMap = Object.fromEntries(users.map(u => [u.id, u]));

const riskLevels = ["低", "中", "高"];
// 预警生命周期：提交 → 确认 → 处理（放行/召回）→ 复核 → 关闭
// “待复核”必须先 review 进入“已复核”，未复核直接关闭会被明确拒绝（review_required）
const alertStatuses = ["待确认", "已确认", "处理中", "待复核", "已复核", "已关闭"];
const alertTransitions = {
  "待确认": ["confirm"],
  "已确认": ["start"],
  "处理中": ["release", "recall"],
  "待复核": ["review"],
  "已复核": ["close"]
};
// 提交人回避：放行、召回与复核都不能由预警提交人本人执行
const recusalActions = ["release", "recall", "review"];
const retestStatuses = ["待复测", "已完成"];

const newSeed = () => {
  const db = structuredClone(seed);
  db.schemaVersion = SCHEMA_VERSION;
  db.revision = 0;
  db.batches = [
    {
      id: "B-061",
      material: "黄山松烟料",
      supplier: "徽州松烟坊",
      arrivedAt: "2026-06-01",
      createdAt: "2026-06-01T08:00:00.000Z"
    },
    {
      id: "B-062",
      material: "桐油炽子",
      supplier: "芜湖油烟行",
      arrivedAt: "2026-06-05",
      createdAt: "2026-06-05T08:00:00.000Z"
    }
  ];
  db.samples = [
    { id: "SY-061-A", batchId: "B-061", retainedAt: "2026-06-02", createdAt: "2026-06-02T08:00:00.000Z" },
    { id: "SY-061-B", batchId: "B-061", retainedAt: "2026-06-02", createdAt: "2026-06-02T08:05:00.000Z" },
    { id: "SY-062-A", batchId: "B-062", retainedAt: "2026-06-06", createdAt: "2026-06-06T08:00:00.000Z" }
  ];
  db.trials = [
    {
      id: "T-1001",
      itemCode: "IS-001",
      batchId: "B-061",
      sampleId: "SY-061-A",
      at: "2026-06-11T02:00:00.000Z",
      paper: "宣纸",
      water: "20滴",
      speed: "快",
      colorLayer: "清透",
      sediment: "无",
      score: 86
    }
  ];
  db.samples[0].usedTrialId = "T-1001";
  db.alerts = [];
  db.freezes = [];
  db.retests = [];
  db.audit = [];
  return db;
};

/* ---------------- 持久层：迁移 + 原子写 + 写互斥 ---------------- */

let writeChain = Promise.resolve();

async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    await writeFile(dbPath, JSON.stringify(newSeed(), null, 2));
  }
  const db = JSON.parse(await readFile(dbPath, "utf8"));
  return migrate(db);
}

// 迁移只新增追溯域集合，绝不改写旧 items / 旧字段
function migrate(db) {
  let changed = false;
  if (typeof db.schemaVersion !== "number") { db.schemaVersion = 1; changed = true; }
  for (const key of ["batches", "samples", "trials", "alerts", "freezes", "retests", "audit"]) {
    if (!Array.isArray(db[key])) { db[key] = []; changed = true; }
  }
  if (typeof db.revision !== "number") { db.revision = 0; changed = true; }
  if (db.schemaVersion < SCHEMA_VERSION) { db.schemaVersion = SCHEMA_VERSION; changed = true; }
  return { db, changed };
}

// 临时文件 + rename：写失败时旧文件不动，预警/影响/冻结/审计不会部分落盘
async function saveDbAtomic(db, { failBeforeRename = false } = {}) {
  const tmp = dbPath + ".tmp-" + process.pid + "-" + (++saveDbAtomic.seq);
  await writeFile(tmp, JSON.stringify(db, null, 2));
  if (failBeforeRename) {
    await unlink(tmp);
    const err = new Error("simulated disk failure before rename");
    err.code = "SIMULATED_WRITE_FAIL";
    throw err;
  }
  await rename(tmp, dbPath);
}
saveDbAtomic.seq = 0;

// 所有变更串行化：在锁内重读最新数据，事务返回结果后一次性原子落盘
function withWrite(mutator, { simulateWriteFail = false } = {}) {
  const run = writeChain.then(async () => {
    const { db } = await loadDb();
    const ctx = { db, audit: [] };
    const result = await mutator(db, ctx);
    if (ctx.audit.length) db.audit.push(...ctx.audit);
    db.revision = (db.revision || 0) + 1;
    // 临时文件已写完但 rename 前失败：旧主文件保持上一完整版本，无部分落盘
    await saveDbAtomic(db, { failBeforeRename: simulateWriteFail });
    return result;
  });
  // 不让上一次失败中断后续排队的写请求
  writeChain = run.then(() => {}, () => {});
  return run;
}

async function body(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}
function send(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data, null, 2));
}
function html(res, text) {
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(text);
}
function nowIso() { return new Date().toISOString(); }
function seqId(db, key, prefix) {
  const n = (db._seq ||= {});
  n[key] = (n[key] || 1000) + 1;
  return prefix + n[key];
}
class ApiError extends Error {
  constructor(status, code, extra) { super(code); this.status = status; this.code = code; Object.assign(this, extra); }
}
function addAudit(ctx, { action, entityType, entityId, actor, detail = {} }) {
  ctx.audit.push({ at: nowIso(), action, entityType, entityId, actor, detail });
}

// 幂等键缓存（进程内）：同一 key+actor 的重复提交直接返回首次结果
const idemCache = new Map();

/* ---------------- 领域派生/校验 ---------------- */

function freezeSet(db) {
  return new Set(db.freezes.filter(f => f.status === "冻结中").map(f => f.targetKey));
}
function batchFrozen(db, batchId) {
  return db.freezes.some(f => f.status === "冻结中" && f.targetType === "batch" && f.targetId === batchId);
}
function itemFrozen(db, code) {
  return db.freezes.some(f => f.status === "冻结中" && f.targetType === "item" && f.targetId === code);
}
function findItem(db, ref) {
  return db.items.find(x => x.id === ref || x.code === ref);
}
function getUser(req) {
  const id = req.headers["x-user-id"];
  return userMap[id] ? userMap[id] : null;
}
function requireUser(req) {
  const u = getUser(req);
  if (!u) throw new ApiError(401, "unauthorized", { error: "缺少或无效的 x-user-id" });
  return u;
}
function trialView(db, t) {
  const frozen = freezeSet(db);
  const keys = [
    "batch:" + t.batchId,
    "sample:" + t.sampleId,
    "item:" + t.itemCode,
    "trial:" + t.id
  ];
  return { ...t, frozen: keys.some(k => frozen.has(k)) };
}
function itemSummarize(db, item) {
  const logCount = (item.logs || []).length + (item.tasks || []).reduce((n, t) => n + (t.logs || []).length, 0);
  const frozen = itemFrozen(db, item.code || item.id);
  return { ...item, logCount, frozen, frozenReason: frozen ? freezeReasonFor(db, "item", item.code || item.id) : null };
}
function freezeReasonFor(db, targetType, targetId) {
  const f = db.freezes.find(x => x.status === "冻结中" && x.targetType === targetType && x.targetId === targetId);
  return f ? f.reason : null;
}

// 计算某批次的影响范围（预警提交时快照）
function impactOfBatch(db, batchId, alertId, reason) {
  const samples = db.samples.filter(s => s.batchId === batchId).map(s => s.id);
  const trials = db.trials.filter(t => t.batchId === batchId);
  const itemCodes = [...new Set(trials.map(t => t.itemCode))];
  const targets = [];
  const push = (targetType, targetId, label) => targets.push({ targetType, targetId, label });
  push("batch", batchId, "原料批次");
  for (const id of samples) push("sample", id, "留样");
  for (const t of trials) push("trial", t.id, "试磨记录");
  for (const code of itemCodes) push("item", code, "墨锭");
  return {
    batchId,
    affectedTrials: trials.length,
    affectedItems: itemCodes.length,
    affectedSamples: samples.length,
    count: targets.length,
    targets: targets.map(t => ({ alertId, reason, status: "冻结中", at: nowIso(), ...t }))
  };
}

function alertView(db, a) {
  const frozenCount = db.freezes.filter(f => f.alertId === a.id && f.status === "冻结中").length;
  return { ...a, frozenCount };
}

/* ---------------- 旧页面（保留现有入口，增加追溯台链接） ---------------- */

const fields = [["code","墨锭编号","text"],["smokeSource","烟料来源","text"],["glueRatio","胶料比例","text"],["ageYears","存放年限","number"],["storage","存放位置","text"]];
const stages = ["待试磨","已试磨","重点观察"];
const statLabels = ["待试磨","已试磨","重点观察"];
const extraFields = [["paper","试磨纸张"],["water","加水量"],["speed","出墨速度"],["colorLayer","墨色层次"],["sediment","沉淀情况"],["score","评分"]];

function newId() { return "IS-" + Date.now(); }
function computeStats(items) {
  const stats = Object.fromEntries(statLabels.map(label => [label, 0]));
  for (const item of items) {
    if (stats[item.status] !== undefined) stats[item.status] += 1;
  }
  return stats;
}

function page() {
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>墨锭试磨室</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#20241f; --muted:#687066; --line:#d4ddd0; --accent:#526f43; --warn:#9b4937; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; gap:16px; align-items:center; }
    h1 { margin:0; font-size:26px; } h2 { margin:0 0 12px; font-size:18px; } main { display:grid; grid-template-columns:380px 1fr; gap:22px; padding:22px 28px; }
    form,.panel,.card,.stat { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; } textarea { min-height:68px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:10px 13px; font-weight:700; cursor:pointer; } button.secondary { background:#69736a; }
    .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:10px; margin-bottom:14px; } .stat strong { display:block; font-size:24px; }
    .toolbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; } .toolbar select,.toolbar input { width:auto; min-width:160px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:12px; } .card { display:grid; gap:8px; }
    .meta { color:var(--muted); font-size:13px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; }
    .logs { border-top:1px solid var(--line); padding-top:8px; max-height:90px; overflow:auto; } .warn { color:var(--warn); font-weight:700; }
    .frozen { border-left:4px solid var(--warn); }
    a { color:var(--accent); font-weight:700; }
    @media (max-width:900px){ header{display:block;padding:18px 16px;} main{grid-template-columns:1fr;padding:16px;} }
  </style>
</head>
<body>
  <header><div><h1>墨锭试磨室</h1><div class="meta">墨锭建档、试磨记录和评分统计 · <a href="/risk">进入原料风险追溯台 →</a></div></div><button id="reload">刷新</button></header>
  <main>
    <section>
      <form id="createForm"><h2>新增墨锭</h2><div id="fields"></div><label>初始状态</label><select name="status">${stages.map(s => '<option>'+s+'</option>').join('')}</select><button>保存墨锭</button></form>
      <form id="actionForm" style="margin-top:14px"><h2>创建试磨记录</h2><label>选择墨锭</label><select name="id" id="itemSelect"></select>
        <label>原料批次（追溯绑定）</label><select name="batchId" id="batchSelect"><option value="">不绑定（旧方式）</option></select>
        <label>留样编号（每支留样仅可使用一次）</label><select name="sampleId" id="sampleSelect"><option value="">不绑定</option></select>
        <div id="extraFields"></div><button>提交记录</button></form>
    </section>
    <section>
      <div class="stats" id="stats"></div>
      <div class="toolbar"><select id="statusFilter"><option value="">全部状态</option>${stages.map(s => '<option>'+s+'</option>').join('')}</select><input id="search" placeholder="搜索编号或关键词"></div>
      <div class="panel"><h2>选择墨锭后录入试磨记录，系统会保留多次试磨结果并更新评分状态。</h2><div class="grid" id="cards"></div></div>
    </section>
  </main>
  <script>
    const fields = [["code","墨锭编号","text"],["smokeSource","烟料来源","text"],["glueRatio","胶料比例","text"],["ageYears","存放年限","number"],["storage","存放位置","text"]];
    const stages = ["待试磨","已试磨","重点观察"];
    const extraFields = [["paper","试磨纸张"],["water","加水量"],["speed","出墨速度"],["colorLayer","墨色层次"],["sediment","沉淀情况"],["score","评分"]];
    const createForm = document.querySelector('#createForm');
    const actionForm = document.querySelector('#actionForm');
    const cards = document.querySelector('#cards');
    const statsEl = document.querySelector('#stats');
    const itemSelect = document.querySelector('#itemSelect');
    const batchSelect = document.querySelector('#batchSelect');
    const sampleSelect = document.querySelector('#sampleSelect');
    let items = [], trace = { batches:[], samples:[], trials:[] };
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? { ...options, headers:{ 'Content-Type':'application/json' } } : options);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || '请求失败');
      return data;
    }
    function renderForms() {
      document.querySelector('#fields').innerHTML = fields.map(([key,label,type]) => '<label>'+label+'</label><input name="'+key+'" type="'+type+'" '+(key==='code'?'required':'')+'>').join('');
      document.querySelector('#extraFields').innerHTML = extraFields.map(([key,label]) => '<label>'+label+'</label><input name="'+key+'">').join('');
    }
    function renderBatchSelects() {
      batchSelect.innerHTML = '<option value="">不绑定（旧方式）</option>' + trace.batches.map(b => '<option value="'+b.id+'">'+b.id+' · '+b.material+'</option>').join('');
      fillSamples();
    }
    function fillSamples() {
      const used = new Set(trace.trials.map(t => t.sampleId));
      const opts = trace.samples.filter(s => !batchSelect.value || s.batchId === batchSelect.value)
        .map(s => '<option value="'+s.id+'" '+(used.has(s.id)?'disabled':'')+'>'+s.id+(used.has(s.id)?'（已使用）':'')+'</option>').join('');
      sampleSelect.innerHTML = '<option value="">不绑定</option>' + opts;
    }
    batchSelect.onchange = fillSamples;
    function render() {
      itemSelect.innerHTML = items.map(item => '<option value="'+(item.id || item.code)+'">'+(item.code || item.id)+' · '+(item.name || item.shipType || item.source || item.plateSize || '')+'</option>').join('');
      const stats = Object.fromEntries(stages.map(s => [s, items.filter(i => i.status === s).length]));
      statsEl.innerHTML = Object.entries(stats).map(([k,v]) => '<div class="stat"><span>'+k+'</span><strong>'+v+'</strong></div>').join('');
      const status = document.querySelector('#statusFilter').value;
      const q = document.querySelector('#search').value.trim();
      const visible = items.filter(item => (!status || item.status === status) && (!q || JSON.stringify(item).includes(q)));
      cards.innerHTML = visible.map(item => cardHtml(item)).join('');
      document.querySelectorAll('[data-status]').forEach(sel => sel.onchange = async () => { try { await api('/api/items/'+sel.dataset.status, { method:'PATCH', body: JSON.stringify({ status: sel.value }) }); await load(); } catch(e){ alert(e.message); } });
      document.querySelectorAll('[data-note]').forEach(btn => btn.onclick = async () => { const id = btn.dataset.note; const note = prompt('记录备注'); if (note) { await api('/api/items/'+id+'/logs', { method:'POST', body: JSON.stringify({ step:'备注', note }) }); await load(); } });
    }
    function cardHtml(item) {
      const main = fields.slice(0,4).map(([key,label]) => '<div><b>'+label+'</b> '+(item[key] ?? '')+'</div>').join('');
      const tasks = (item.tasks || []).map(t => '<div class="meta">任务 '+t.position+' · '+t.status+' · '+t.tension+'</div>').join('');
      const logs = (item.logs || []).slice(-4).map(l => '<div>'+l.step+'：'+l.note+'</div>').join('');
      const fz = item.frozen ? '<span class="pill warn">已冻结：'+(item.frozenReason||'原料风险')+'</span>' : '';
      return '<article class="card'+(item.frozen?' frozen':'')+'"><h3>'+(item.code || item.id)+'</h3><span class="pill">'+item.status+'</span>'+fz+main+tasks+'<label>状态</label><select data-status="'+(item.id || item.code)+'">'+stages.map(s => '<option '+(s===item.status?'selected':'')+'>'+s+'</option>').join('')+'</select><button class="secondary" data-note="'+(item.id || item.code)+'">追加备注</button><div class="logs meta">'+(logs || '暂无记录')+'</div></article>';
    }
    async function load() {
      [items, trace] = await Promise.all([api('/api/items'), api('/api/trace')]);
      renderBatchSelects(); render();
    }
    createForm.onsubmit = async event => { event.preventDefault(); await api('/api/items', { method:'POST', body: JSON.stringify(Object.fromEntries(new FormData(createForm).entries())) }); createForm.reset(); await load(); };
    actionForm.onsubmit = async event => {
      event.preventDefault();
      const data = Object.fromEntries(new FormData(actionForm).entries());
      const ref = data.id; delete data.id;
      try {
        if (data.batchId || data.sampleId) {
          if (!data.batchId || !data.sampleId) return alert('绑定试磨必须同时选择批次和留样');
          await api('/api/trials', { method:'POST', headers:{'Content-Type':'application/json','x-user-id':'u01'}, body: JSON.stringify({ itemCode: ref, ...data }) });
        } else {
          await api('/api/items/'+ref+'/action', { method:'POST', body: JSON.stringify(data) });
        }
        actionForm.reset(); await load();
      } catch(e){ alert(e.message); }
    };
    document.querySelector('#statusFilter').onchange = render; document.querySelector('#search').oninput = render; document.querySelector('#reload').onclick = load;
    renderForms(); load();
  </script>
</body>
</html>`;
}

/* ---------------- 风险追溯台页面 ---------------- */

function riskPage() {
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>原料风险追溯台 · 墨锭试磨室</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#20241f; --muted:#687066; --line:#d4ddd0; --accent:#526f43; --warn:#9b4937; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:20px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; gap:16px; align-items:center; flex-wrap:wrap; }
    h1 { margin:0; font-size:24px; } h2 { margin:0 0 10px; font-size:17px; } h3 { margin:0; font-size:16px; }
    main { padding:20px 28px; display:grid; grid-template-columns:360px 1fr; gap:18px; align-items:start; }
    .panel,form { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
    .col { display:grid; gap:14px; }
    label { display:block; margin:9px 0 4px; color:var(--muted); font-size:13px; }
    input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:8px; font:inherit; background:#fff; } textarea { min-height:56px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:9px 12px; font-weight:700; cursor:pointer; font-size:14px; }
    button.secondary { background:#69736a; } button.danger { background:var(--warn); } button:disabled { opacity:.45; cursor:not-allowed; }
    .row { display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
    a { color:var(--accent); }
    .meta { color:var(--muted); font-size:13px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 9px; font-size:12px; margin:2px 4px 2px 0; }
    .risk-高 { background:#f6e2dd; color:var(--warn); border-color:#d9a99d; font-weight:700; }
    .risk-中 { background:#fbf1dc; color:#8a6417; border-color:#e0c489; }
    .risk-低 { background:#e6efe0; color:#3f5c33; border-color:#aec3a2; }
    .frozen { border-left:4px solid var(--warn); }
    .alert { display:grid; gap:7px; margin-bottom:12px; }
    .kvs { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:6px; }
    .kv b { display:block; font-size:18px; }
    table { width:100%; border-collapse:collapse; font-size:13px; } th,td { text-align:left; border-bottom:1px solid var(--line); padding:6px 8px; vertical-align:top; }
    .timeline { border-left:2px solid var(--line); padding-left:12px; display:grid; gap:6px; }
    .toast { position:fixed; left:50%; bottom:18px; transform:translateX(-50%); background:#20241f; color:#fff; padding:10px 16px; border-radius:8px; font-size:14px; opacity:0; transition:.2s; pointer-events:none; max-width:90vw; }
    .toast.show { opacity:.95; } .toast.err { background:var(--warn); }
    .tabs { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; }
    .tabs button.on { background:#20241f; }
    @media (max-width:900px){ header{padding:14px 14px;} main{grid-template-columns:1fr;padding:14px;} table { font-size:12px; } }
  </style>
</head>
<body>
  <header>
    <div><h1>原料风险追溯台</h1><div class="meta">批次 · 留样 · 试磨绑定 · 预警冻结 · 放行/召回复测</div></div>
    <div class="row">
      <label style="margin:0">操作人</label>
      <select id="actor" style="width:auto;min-width:150px"></select>
      <a href="/">← 返回墨锭试磨室</a>
      <button class="secondary" id="reload">刷新</button>
    </div>
  </header>
  <main>
    <div class="col">
      <form id="batchForm"><h2>登记原料批次</h2>
        <label>原料</label><input name="material" required placeholder="如 黄山松烟料">
        <label>供应商</label><input name="supplier">
        <button>登记批次</button>
      </form>
      <form id="sampleForm"><h2>批次留样</h2>
        <label>所属批次</label><select name="batchId" id="sampleBatch"></select>
        <label>留样日期</label><input name="retainedAt" type="date">
        <button>登记留样</button>
      </form>
      <form id="trialForm"><h2>试磨绑定（批次+留样，留样唯一）</h2>
        <label>墨锭</label><select name="itemCode" id="trialItem"></select>
        <label>原料批次</label><select name="batchId" id="trialBatch"></select>
        <label>留样编号</label><select name="sampleId" id="trialSample"></select>
        <div class="row" style="margin-top:8px"><input name="paper" placeholder="纸张" style="flex:1"><input name="water" placeholder="加水量" style="flex:1"></div>
        <div class="row"><input name="speed" placeholder="出墨速度" style="flex:1"><input name="score" type="number" placeholder="评分" style="flex:1"></div>
        <div style="margin-top:8px"><button type="submit">提交绑定试磨</button></div>
      </form>
      <form id="alertForm"><h2>原料异常预警（提交即冻结）</h2>
        <label>原料批次</label><select name="batchId" id="alertBatch"></select>
        <label>风险等级</label><select name="riskLevel"><option>低</option><option selected>中</option><option>高</option></select>
        <label>异常描述</label><textarea name="reason" required placeholder="如 留样检出胶性异常"></textarea>
        <div style="margin-top:8px"><button class="danger">提交预警并冻结</button></div>
      </form>
    </div>
    <div class="col">
      <div class="panel"><div class="kvs" id="overview"></div></div>
      <div class="panel">
        <div class="tabs">
          <button data-tab="alerts" class="on">预警处置</button>
          <button data-tab="trials">试磨绑定</button>
          <button data-tab="freezes">冻结清单</button>
          <button data-tab="retests">复测任务</button>
          <button data-tab="audit">审计</button>
        </div>
        <div id="tabAlerts"></div>
        <div id="tabTrials" hidden></div>
        <div id="tabFreezes" hidden></div>
        <div id="tabRetests" hidden></div>
        <div id="tabAudit" hidden></div>
      </div>
    </div>
  </main>
  <div class="toast" id="toast"></div>
  <script>
    const users = ${JSON.stringify(users)};
    const statuses = ${JSON.stringify(alertStatuses)};
    const actorSel = document.querySelector('#actor');
    actorSel.innerHTML = users.map(u => '<option value="'+u.id+'">'+u.name+'（'+u.role+'）</option>').join('');
    actorSel.value = localStorage.getItem('riskActor') || users[0].id;
    actorSel.onchange = () => { localStorage.setItem('riskActor', actorSel.value); render(); };
    const actor = () => actorSel.value;
    let state = { batches:[], samples:[], trials:[], alerts:[], freezes:[], retests:[], items:[] };

    async function api(path, options = {}) {
      const opts = { ...options, headers: { 'Content-Type':'application/json', 'x-user-id': actor(), ...(options.headers||{}) } };
      const res = await fetch(path, opts);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) { const e = new Error(data.error || data.code || '请求失败'); e.code = data.code; throw e; }
      return data;
    }
    function toast(msg, isErr) { const t = document.querySelector('#toast'); t.textContent = msg; t.className = 'toast show' + (isErr?' err':''); setTimeout(() => t.className = 'toast', 2600); }
    function esc(s){ return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

    async function load() {
      state = await api('/api/trace');
      fillSelects(); render();
    }
    function fillSelects() {
      const batches = state.batches.map(b => '<option value="'+b.id+'">'+b.id+' · '+esc(b.material)+'</option>').join('');
      document.querySelector('#sampleBatch').innerHTML = batches;
      document.querySelector('#alertBatch').innerHTML = batches;
      document.querySelector('#trialBatch').innerHTML = batches;
      document.querySelector('#trialItem').innerHTML = state.items.map(i => '<option value="'+esc(i.code||i.id)+'">'+esc(i.code||i.id)+'</option>').join('');
      fillSamples();
    }
    function fillSamples() {
      const bid = document.querySelector('#trialBatch').value;
      const used = new Set(state.trials.map(t => t.sampleId));
      document.querySelector('#trialSample').innerHTML = state.samples
        .filter(s => s.batchId === bid)
        .map(s => '<option value="'+s.id+'" '+(used.has(s.id)?'disabled':'')+'>'+s.id+(used.has(s.id)?'（已使用）':'')+'</option>').join('');
    }
    document.querySelector('#trialBatch').onchange = fillSamples;

    function render() {
      const frozenSet = new Set(state.freezes.filter(f => f.status === '冻结中').map(f => f.targetKey));
      const activeAlerts = state.alerts.filter(a => a.status !== '已关闭').length;
      const frozen = state.freezes.filter(f => f.status === '冻结中').length;
      const high = state.alerts.filter(a => a.riskLevel === '高' && a.status !== '已关闭').length;
      document.querySelector('#overview').innerHTML = [
        ['在用批次', state.batches.length],['活跃预警', activeAlerts],['冻结对象', frozen],
        ['高风险预警', high],['留样总数', state.samples.length],['待复测', state.retests.filter(r=>r.status==='待复测').length]
      ].map(([k,v]) => '<div class="kv panel" style="padding:8px"><span class="meta">'+k+'</span><b>'+v+'</b></div>').join('');

      renderAlerts(); renderTrials(frozenSet); renderFreezes(); renderRetests(); renderAudit();
    }
    function transitionButtons(a) {
      const next = { '待确认':[['confirm','确认预警','secondary']], '已确认':[['start','开始处理','secondary']],
        '处理中':[['release','审核放行',''],['recall','审核召回','danger']],
        '待复核':[['review','复核通过','secondary']],
        '已复核':[['close','关闭并解冻','']] }[a.status] || [];
      return next.map(([act,label,cls]) => {
        const mine = a.createdBy === actor();
        const reviewBlocked = (act==='release'||act==='recall'||act==='review') && mine;
        const recallBlocked = act==='close' && a.decision==='召回' && state.retests.some(r=>r.alertId===a.id && r.status!=='已完成');
        const dis = reviewBlocked || recallBlocked;
        const title = reviewBlocked ? '提交人需回避，须由其他操作人执行' : (recallBlocked ? '复测全部完成后才能关闭召回预警' : '');
        return '<button class="'+cls+'" data-alert="'+a.id+'" data-act="'+act+'" '+(dis?'disabled title="'+title+'"':'')+'>'+label+'</button>';
      }).join(' ');
    }
    function renderAlerts() {
      document.querySelector('#tabAlerts').innerHTML = state.alerts.length ? state.alerts.map(a => {
        const impact = a.impact || {};
        const tl = (a.timeline || []).map(e => '<div class="meta">'+e.at.slice(0,19).replace('T',' ')+' · '+e.action+' · '+(e.by||'')+(e.note?('：'+esc(e.note)):'')+'</div>').join('');
        const retests = state.retests.filter(r => r.alertId === a.id).map(r => '<span class="pill">'+r.id+' '+r.status+'</span>').join('') || '';
        return '<div class="panel alert '+(a.status!=='已关闭'?'frozen':'')+'">'
          + '<div class="row" style="justify-content:space-between"><h3>'+a.id+'</h3>'
          + '<span><span class="pill risk-'+a.riskLevel+'">风险 '+a.riskLevel+'</span><span class="pill">'+a.status+'</span></span></div>'
          + '<div class="meta">批次 <b>'+a.batchId+'</b> · 提交人 '+esc(a.createdByName)+' · '+a.createdAt.slice(0,16).replace('T',' ')+'</div>'
          + '<div class="kvs"><div class="kv"><span class="meta">影响数量</span><b>'+(impact.count ?? 0)+'</b></div>'
          + '<div class="kv"><span class="meta">涉及试磨/墨锭/留样</span><b>'+(impact.affectedTrials||0)+' / '+(impact.affectedItems||0)+' / '+(impact.affectedSamples||0)+'</b></div>'
          + '<div class="kv"><span class="meta">冻结中</span><b>'+a.frozenCount+'</b></div></div>'
          + '<div class="meta">冻结原因：'+esc(a.reason)+(a.decision?(' · 审核结论：<b>'+a.decision+'</b>'):'')+'</div>'
          + (retests ? '<div class="meta">复测任务：'+retests+'</div>' : '')
          + '<div class="row">'+transitionButtons(a)+'</div>'
          + '<details><summary class="meta">处置时间线 / 召回快照</summary><div class="timeline" style="margin-top:6px">'+tl+'</div>'
          + (a.recallSnapshot?'<pre style="white-space:pre-wrap;font-size:12px">'+esc(JSON.stringify(a.recallSnapshot,null,2))+'</pre>':'')
          + '</details></div>';
      }).join('') : '<div class="meta">暂无预警。</div>';
      document.querySelectorAll('[data-alert]').forEach(btn => btn.onclick = async () => {
        try { await api('/api/alerts/'+btn.dataset.alert+'/transition', { method:'POST', body: JSON.stringify({ action: btn.dataset.act }) }); toast('操作成功'); await load(); }
        catch (e) { toast(e.message, true); }
      });
    }
    function renderTrials() {
      document.querySelector('#tabTrials').innerHTML = '<table><thead><tr><th>试磨</th><th>墨锭</th><th>批次</th><th>留样</th><th>评分</th><th>状态</th></tr></thead><tbody>'
        + state.trials.map(t => '<tr class="'+(t.frozen?'frozen':'')+'"><td>'+t.id+'</td><td>'+esc(t.itemCode)+'</td><td>'+t.batchId+'</td><td>'+t.sampleId+'</td><td>'+(t.score??'')+'</td><td>'+(t.frozen?'<span class="pill risk-高">已冻结</span>':'正常')+'</td></tr>').join('')
        + '</tbody></table>';
    }
    function renderFreezes() {
      document.querySelector('#tabFreezes').innerHTML = '<table><thead><tr><th>对象</th><th>编号</th><th>原因</th><th>预警</th><th>状态</th></tr></thead><tbody>'
        + state.freezes.map(f => '<tr class="'+(f.status==='冻结中'?'frozen':'')+'"><td>'+f.label+'</td><td>'+f.targetId+'</td><td>'+esc(f.reason)+'</td><td>'+f.alertId+'</td><td>'+f.status+'</td></tr>').join('')
        + '</tbody></table>';
    }
    function renderRetests() {
      document.querySelector('#tabRetests').innerHTML = state.retests.length ? state.retests.map(r => '<div class="panel '+(r.status==='待复测'?'frozen':'')+'"><h3>'+r.id+'</h3>'
        + '<div class="meta">预警 '+r.alertId+' · 试磨 '+r.trialId+' · 墨锭 '+esc(r.itemCode)+' · 批次 '+r.batchId+' · 留样 '+r.sampleId+'</div>'
        + '<div class="meta">状态：<b>'+r.status+'</b>'+(r.completedAt?(' · 完成于 '+r.completedAt.slice(0,16).replace('T',' ')):'')+'</div>'
        + (r.status==='待复测'?'<div class="row" style="margin-top:6px"><input id="rt'+r.id+'" placeholder="复测结果" style="flex:1"><input id="rs'+r.id+'" type="number" placeholder="复测评分" style="max-width:130px"><button data-retest="'+r.id+'">完成复测</button></div>':'')
        + '</div>').join('') : '<div class="meta">暂无复测任务。</div>';
      document.querySelectorAll('[data-retest]').forEach(btn => btn.onclick = async () => {
        try {
          const result = document.querySelector('#rt'+btn.dataset.retest).value;
          const score = Number(document.querySelector('#rs'+btn.dataset.retest).value || 0);
          await api('/api/retests/'+btn.dataset.retest+'/complete', { method:'POST', body: JSON.stringify({ result, score }) });
          toast('复测已记录'); await load();
        } catch (e) { toast(e.message, true); }
      });
    }
    function renderAudit() {
      document.querySelector('#tabAudit').innerHTML = '<table><thead><tr><th>时间</th><th>操作</th><th>对象</th><th>操作人</th></tr></thead><tbody>'
        + state.audit.slice().reverse().map(e => '<tr><td>'+e.at.slice(0,19).replace('T',' ')+'</td><td>'+e.action+'</td><td>'+e.entityType+' '+esc(e.entityId||'')+'</td><td>'+esc(e.actor)+'</td></tr>').join('')
        + '</tbody></table>';
    }
    document.querySelectorAll('.tabs button').forEach(b => b.onclick = () => {
      document.querySelectorAll('.tabs button').forEach(x => x.classList.remove('on')); b.classList.add('on');
      const map = { alerts:'tabAlerts', trials:'tabTrials', freezes:'tabFreezes', retests:'tabRetests', audit:'tabAudit' };
      Object.entries(map).forEach(([k,id]) => document.querySelector('#'+id).hidden = (k !== b.dataset.tab));
    });

    document.querySelector('#batchForm').onsubmit = async e => { e.preventDefault(); const f = e.target;
      try { await api('/api/batches', { method:'POST', body: JSON.stringify(Object.fromEntries(new FormData(f).entries())) }); toast('批次已登记'); f.reset(); await load(); } catch(err){ toast(err.message,true); } };
    document.querySelector('#sampleForm').onsubmit = async e => { e.preventDefault(); const f = e.target;
      try { await api('/api/samples', { method:'POST', body: JSON.stringify(Object.fromEntries(new FormData(f).entries())) }); toast('留样已登记'); f.reset(); await load(); } catch(err){ toast(err.message,true); } };
    document.querySelector('#trialForm').onsubmit = async e => { e.preventDefault(); const f = e.target;
      try { await api('/api/trials', { method:'POST', body: JSON.stringify(Object.fromEntries(new FormData(f).entries())) }); toast('试磨已绑定'); f.reset(); await load(); } catch(err){ toast(err.message,true); } };
    document.querySelector('#alertForm').onsubmit = async e => { e.preventDefault(); const f = e.target;
      try { const a = await api('/api/alerts', { method:'POST', body: JSON.stringify(Object.fromEntries(new FormData(f).entries())) }); toast('预警已提交，'+a.impact.count+' 个对象已冻结'); f.reset(); await load(); } catch(err){ toast(err.message,true); } };
    document.querySelector('#reload').onclick = load;
    load();
  </script>
</body>
</html>`;
}

/* ---------------- HTTP 服务 ---------------- */

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const p = url.pathname;
    const simulateWriteFail = req.headers["x-simulate"] === "write-fail";

    /* 页面：保留旧入口 "/"，新增 "/risk" */
    if (req.method === "GET" && p === "/") return html(res, page());
    if (req.method === "GET" && p === "/risk") return html(res, riskPage());

    /* 追溯聚合数据（只读） */
    if (req.method === "GET" && p === "/api/trace") {
      const { db } = await loadDb();
      return send(res, 200, {
        batches: db.batches,
        samples: db.samples.map(s => ({ ...s, used: !!s.usedTrialId })),
        trials: db.trials.map(t => trialView(db, t)),
        alerts: db.alerts.map(a => alertView(db, a)),
        freezes: db.freezes,
        retests: db.retests,
        audit: db.audit,
        items: db.items.map(i => ({ code: i.code, id: i.id, status: i.status })),
        revision: db.revision
      });
    }
    if (req.method === "GET" && p === "/api/consistency") {
      const { db } = await loadDb();
      return send(res, 200, consistencyCheck(db));
    }
    if (req.method === "GET" && p === "/api/users") return send(res, 200, users);

    /* 批次 */
    if (req.method === "POST" && p === "/api/batches") {
      const user = requireUser(req);
      const input = await body(req);
      const out = await withWrite((db, ctx) => {
        const id = input.id || seqId(db, "batch", "B-");
        if (db.batches.some(b => b.id === id)) throw new ApiError(409, "batch_duplicate");
        const batch = { id, material: input.material || "", supplier: input.supplier || "", arrivedAt: input.arrivedAt || nowIso().slice(0, 10), createdAt: nowIso() };
        db.batches.push(batch);
        addAudit(ctx, { action: "登记批次", entityType: "batch", entityId: batch.id, actor: user.name });
        return batch;
      }, { simulateWriteFail });
      return send(res, 201, out);
    }

    /* 留样 */
    if (req.method === "POST" && p === "/api/samples") {
      const user = requireUser(req);
      const input = await body(req);
      if (!input.batchId) throw new ApiError(400, "sample_fields_required");
      const out = await withWrite((db, ctx) => {
        if (!db.batches.some(b => b.id === input.batchId)) throw new ApiError(404, "batch_not_found");
        const id = input.id || seqId(db, "sample", "SY-");
        if (db.samples.some(s => s.id === id)) throw new ApiError(409, "sample_duplicate");
        if (batchFrozen(db, input.batchId)) throw new ApiError(409, "batch_frozen");
        const sample = { id, batchId: input.batchId, retainedAt: input.retainedAt || nowIso().slice(0, 10), createdAt: nowIso() };
        db.samples.push(sample);
        addAudit(ctx, { action: "登记留样", entityType: "sample", entityId: sample.id, actor: user.name, detail: { batchId: sample.batchId } });
        return sample;
      }, { simulateWriteFail });
      return send(res, 201, out);
    }

    /* 试磨绑定：同一留样只能使用一次（含并发） */
    if (req.method === "POST" && p === "/api/trials") {
      const user = requireUser(req);
      const input = await body(req);
      const { itemCode, batchId, sampleId } = input;
      if (!itemCode || !batchId || !sampleId) throw new ApiError(400, "trial_bind_fields_required");
      const out = await withWrite((db, ctx) => {
        const item = findItem(db, itemCode);
        if (!item) throw new ApiError(404, "item_not_found");
        if (!db.batches.some(b => b.id === batchId)) throw new ApiError(404, "batch_not_found");
        const sample = db.samples.find(s => s.id === sampleId);
        if (!sample) throw new ApiError(404, "sample_not_found");
        if (sample.batchId !== batchId) throw new ApiError(400, "sample_batch_mismatch");
        if (sample.usedTrialId || db.trials.some(t => t.sampleId === sampleId)) throw new ApiError(409, "sample_already_used");
        if (batchFrozen(db, batchId)) throw new ApiError(409, "batch_frozen");
        if (itemFrozen(db, item.code || item.id)) throw new ApiError(409, "item_frozen");
        const score = Number(input.score || 0);
        const trial = {
          id: seqId(db, "trial", "T-"),
          itemCode: item.code || item.id,
          batchId, sampleId,
          at: nowIso(),
          paper: input.paper || "", water: input.water || "", speed: input.speed || "",
          colorLayer: input.colorLayer || "", sediment: input.sediment || "", score
        };
        db.trials.push(trial);
        sample.usedTrialId = trial.id;
        // 同步到旧墨锭记录，保持旧页面可用
        item.logs ||= [];
        item.logs.push({ at: trial.at, step: "试磨", note: "批次" + batchId + " 留样" + sampleId + "，评分" + score, score });
        item.tests ||= [];
        item.tests.push({ at: trial.at, ...Object.fromEntries(extraFields.map(([k]) => [k, input[k] ?? ""])), score });
        item.status = score >= 85 ? "已试磨" : "重点观察";
        addAudit(ctx, { action: "试磨绑定", entityType: "trial", entityId: trial.id, actor: user.name, detail: { batchId, sampleId, itemCode: trial.itemCode } });
        return trial;
      }, { simulateWriteFail });
      return send(res, 201, out);
    }

    /* 预警：提交即沿批次/留样关系冻结影响范围（单一事务原子落盘） */
    if (req.method === "POST" && p === "/api/alerts") {
      const user = requireUser(req);
      const input = await body(req);
      if (!input.batchId || !input.reason) throw new ApiError(400, "alert_fields_required");
      if (input.riskLevel && !riskLevels.includes(input.riskLevel)) throw new ApiError(400, "bad_risk_level");
      const idemKey = "alert:" + user.id + ":" + input.batchId + ":" + input.reason;
      if (idemCache.has(idemKey)) {
        const hit = idemCache.get(idemKey);
        return send(res, 200, { ...hit, idempotentReplay: true });
      }
      const out = await withWrite((db, ctx) => {
        if (!db.batches.some(b => b.id === input.batchId)) throw new ApiError(404, "batch_not_found");
        if (db.alerts.some(a => a.batchId === input.batchId && a.status !== "已关闭")) {
          throw new ApiError(409, "active_alert_exists");
        }
        const alert = {
          id: seqId(db, "alert", "AL-"),
          batchId: input.batchId,
          riskLevel: input.riskLevel || "中",
          reason: String(input.reason),
          status: "待确认",
          createdBy: user.id,
          createdByName: user.name,
          createdAt: nowIso(),
          impact: null,
          timeline: [],
          decision: null,
          recallSnapshot: null
        };
        const impact = impactOfBatch(db, alert.batchId, alert.id, alert.reason);
        alert.impact = {
          count: impact.count,
          affectedSamples: impact.affectedSamples,
          affectedTrials: impact.affectedTrials,
          affectedItems: impact.affectedItems
        };
        db.alerts.push(alert);
        // 并发冻结只成功一次：targetKey 唯一，重复对象不重复冻结
        for (const f of impact.targets) {
          const targetKey = f.targetType + ":" + f.targetId;
          if (db.freezes.some(x => x.targetKey === targetKey && x.status === "冻结中")) continue;
          db.freezes.push({ id: seqId(db, "freeze", "FZ-"), alertId: alert.id, targetKey, ...f });
        }
        alert.timeline.push({ at: nowIso(), action: "提交预警", by: user.name, note: "冻结影响范围 " + impact.count + " 项" });
        addAudit(ctx, { action: "提交预警", entityType: "alert", entityId: alert.id, actor: user.name, detail: { batchId: alert.batchId, riskLevel: alert.riskLevel, impact: impact.count } });
        return alertView(db, alert);
      }, { simulateWriteFail });
      idemCache.set(idemKey, out);
      return send(res, 201, out);
    }

    /* 预警状态机：确认/处理/放行/召回/复核/关闭 */
    const transitionMatch = p.match(/^\/api\/alerts\/([^/]+)\/transition$/);
    if (transitionMatch && req.method === "POST") {
      const user = requireUser(req);
      const input = await body(req);
      const action = input.action;
      const allowed = ["confirm", "start", "release", "recall", "review", "close"];
      if (!allowed.includes(action)) throw new ApiError(400, "unknown_action");
      const out = await withWrite((db, ctx) => {
        const alert = db.alerts.find(a => a.id === transitionMatch[1]);
        if (!alert) throw new ApiError(404, "alert_not_found");
        const legal = (alertTransitions[alert.status] || []).includes(action);
        // 明确失败：待复核状态必须先复核，未复核直接关闭不允许（状态与审计均不变）
        if (!legal && action === "close" && alert.status === "待复核") {
          throw new ApiError(409, "review_required");
        }
        if (!legal) throw new ApiError(409, "illegal_transition", { from: alert.status, action });
        // 重复提交：同状态重复进入同一动作直接拒绝
        const last = alert.timeline[alert.timeline.length - 1];
        if (last && last.actionKey === action) throw new ApiError(409, "duplicate_transition");

        if (recusalActions.includes(action) && alert.createdBy === user.id) {
          throw new ApiError(403, "submitter_cannot_review");
        }

        const note = input.note || "";
        if (action === "confirm") {
          alert.status = "已确认";
          alert.timeline.push({ at: nowIso(), actionKey: action, action: "确认预警", by: user.name, note });
          addAudit(ctx, { action: "确认预警", entityType: "alert", entityId: alert.id, actor: user.name });
        } else if (action === "start") {
          alert.status = "处理中";
          alert.timeline.push({ at: nowIso(), actionKey: action, action: "开始处理", by: user.name, note });
          addAudit(ctx, { action: "开始处理", entityType: "alert", entityId: alert.id, actor: user.name });
        } else if (action === "release" || action === "recall") {
          // 审核只能放行或召回
          alert.decision = action === "release" ? "放行" : "召回";
          alert.status = "待复核";
          if (action === "recall") {
            // 召回：保留原结论快照并生成复测任务
            const trials = db.trials.filter(t => t.batchId === alert.batchId);
            alert.recallSnapshot = {
              at: nowIso(),
              alertId: alert.id,
              batchId: alert.batchId,
              previousDecision: "召回前原试磨结论",
              trials: structuredClone(trials),
              items: structuredClone([...new Set(trials.map(t => t.itemCode))].map(code => {
                const it = findItem(db, code);
                return it ? { code, status: it.status, tests: structuredClone(it.tests || []) } : { code };
              }))
            };
            for (const t of trials) {
              if (db.retests.some(r => r.trialId === t.id && r.alertId === alert.id)) continue;
              db.retests.push({
                id: seqId(db, "retest", "RT-"),
                alertId: alert.id,
                trialId: t.id,
                itemCode: t.itemCode,
                batchId: t.batchId,
                sampleId: t.sampleId,
                status: "待复测",
                createdAt: nowIso(),
                completedAt: null,
                result: null,
                score: null
              });
            }
          }
          alert.timeline.push({ at: nowIso(), actionKey: action, action: "审核" + alert.decision, by: user.name, note });
          addAudit(ctx, { action: "审核" + alert.decision, entityType: "alert", entityId: alert.id, actor: user.name, detail: { retests: action === "recall" ? db.retests.filter(r => r.alertId === alert.id).length : 0 } });
        } else if (action === "review") {
          // 复核通过：进入“已复核”，冻结保持至关闭；记录复核意见
          alert.status = "已复核";
          alert.reviewedBy = user.name;
          alert.reviewedAt = nowIso();
          alert.timeline.push({ at: alert.reviewedAt, actionKey: action, action: "复核通过", by: user.name, note });
          addAudit(ctx, { action: "复核", entityType: "alert", entityId: alert.id, actor: user.name });
        } else if (action === "close") {
          // 前置门禁：必须已复核；召回预警还要求复测全部完成
          if (alert.status !== "已复核") throw new ApiError(409, "review_required");
          if (alert.decision === "召回") {
            const pending = db.retests.filter(r => r.alertId === alert.id && r.status !== "已完成");
            if (pending.length) throw new ApiError(409, "retest_pending", { pending: pending.length });
          }
          alert.status = "已关闭";
          for (const f of db.freezes) if (f.alertId === alert.id && f.status === "冻结中") f.status = "已解冻";
          alert.timeline.push({ at: nowIso(), actionKey: action, action: "关闭并解冻", by: user.name, note });
          addAudit(ctx, { action: "关闭预警", entityType: "alert", entityId: alert.id, actor: user.name });
        }
        return alertView(db, alert);
      }, { simulateWriteFail });
      return send(res, 200, out);
    }

    /* 复测完成 */
    const retestMatch = p.match(/^\/api\/retests\/([^/]+)\/complete$/);
    if (retestMatch && req.method === "POST") {
      const user = requireUser(req);
      const input = await body(req);
      const out = await withWrite((db, ctx) => {
        const r = db.retests.find(x => x.id === retestMatch[1]);
        if (!r) throw new ApiError(404, "retest_not_found");
        if (r.status === "已完成") throw new ApiError(409, "retest_already_done");
        r.status = "已完成";
        r.completedAt = nowIso();
        r.result = input.result || "";
        r.score = input.score == null || input.score === "" ? null : Number(input.score);
        addAudit(ctx, { action: "完成复测", entityType: "retest", entityId: r.id, actor: user.name, detail: { score: r.score } });
        return r;
      }, { simulateWriteFail });
      return send(res, 200, out);
    }

    /* ---------------- 旧 API（保留入口，冻结写入被拦截） ---------------- */
    if (req.method === "GET" && p === "/api/items") {
      const { db } = await loadDb();
      return send(res, 200, db.items.map(i => itemSummarize(db, i)));
    }
    if (req.method === "POST" && p === "/api/items") {
      const input = await body(req);
      const out = await withWrite((db, ctx) => {
        const item = { id: newId(), ...input, logs: [{ at: new Date().toISOString(), step: "建档", note: "创建墨锭" }] };
        db.items.unshift(item);
        const u = getUser(req);
        addAudit(ctx, { action: "新增墨锭", entityType: "item", entityId: item.code || item.id, actor: u ? u.name : "匿名" });
        return item;
      }, { simulateWriteFail });
      return send(res, 201, out);
    }
    const patch = p.match(/^\/api\/items\/([^/]+)$/);
    if (patch && req.method === "PATCH") {
      const input = await body(req);
      const out = await withWrite((db, ctx) => {
        const item = findItem(db, patch[1]);
        if (!item) throw new ApiError(404, "item_not_found");
        if (itemFrozen(db, item.code || item.id)) throw new ApiError(409, "item_frozen");
        Object.assign(item, input);
        item.logs ||= [];
        item.logs.push({ at: new Date().toISOString(), step: "状态", note: "更新为" + item.status });
        const u = getUser(req);
        addAudit(ctx, { action: "更新墨锭", entityType: "item", entityId: item.code || item.id, actor: u ? u.name : "匿名" });
        return item;
      }, { simulateWriteFail });
      return send(res, 200, out);
    }
    const logRoute = p.match(/^\/api\/items\/([^/]+)\/logs$/);
    if (logRoute && req.method === "POST") {
      const input = await body(req);
      const out = await withWrite((db, ctx) => {
        const item = findItem(db, logRoute[1]);
        if (!item) throw new ApiError(404, "item_not_found");
        if (itemFrozen(db, item.code || item.id)) throw new ApiError(409, "item_frozen");
        item.logs ||= [];
        item.logs.push({ at: new Date().toISOString(), step: input.step || "记录", note: input.note || "" });
        const u = getUser(req);
        addAudit(ctx, { action: "追加备注", entityType: "item", entityId: item.code || item.id, actor: u ? u.name : "匿名" });
        return item;
      }, { simulateWriteFail });
      return send(res, 201, out);
    }
    // 旧试磨入口保留：未绑定时按旧逻辑工作；若携带 batchId/sampleId 则走绑定规则
    const action = p.match(/^\/api\/items\/([^/]+)\/action$/);
    if (action && req.method === "POST") {
      const input = await body(req);
      if (input.batchId || input.sampleId) throw new ApiError(400, "use /api/trials for binding");
      const out = await withWrite((db) => {
        const item = findItem(db, action[1]);
        if (!item) throw new ApiError(404, "item_not_found");
        if (itemFrozen(db, item.code || item.id)) throw new ApiError(409, "item_frozen");
        item.logs ||= [];
        const score = Number(input.score || 0);
        item.tests ||= [];
        item.tests.push({ at: new Date().toISOString(), ...input, score });
        item.status = score >= 85 ? "已试磨" : "重点观察";
        item.logs.push({ at: new Date().toISOString(), step: "试磨", note: (input.paper || "试纸") + "，评分" + score, score });
        return item;
      }, { simulateWriteFail });
      return send(res, 201, out);
    }
    if (req.method === "GET" && p === "/api/stats") {
      const { db } = await loadDb();
      return send(res, 200, computeStats(db.items));
    }

    send(res, 404, { error: "not_found" });
  } catch (error) {
    if (error instanceof SyntaxError) return send(res, 400, { error: "bad_json" });
    const status = error.status || 500;
    send(res, status, { error: error.code || error.message });
  }
});

/* ---------------- 重启一致性自检（不修改数据） ---------------- */

function consistencyCheck(db) {
  const problems = [];
  // 1. 留样唯一使用：sample.usedTrialId 与 trials 一致，且至多一条
  for (const s of db.samples) {
    const usersTrial = db.trials.filter(t => t.sampleId === s.id);
    if (usersTrial.length > 1) problems.push("sample_multi_use:" + s.id);
    if (usersTrial.length === 1 && s.usedTrialId !== usersTrial[0].id) problems.push("sample_link_stale:" + s.id);
    if (usersTrial.length === 0 && s.usedTrialId) problems.push("sample_link_dangling:" + s.id);
  }
  // 2. 试磨关系完整
  for (const t of db.trials) {
    if (!db.batches.some(b => b.id === t.batchId)) problems.push("trial_batch_missing:" + t.id);
    if (!db.samples.some(s => s.id === t.sampleId)) problems.push("trial_sample_missing:" + t.id);
    if (!db.items.some(i => (i.code || i.id) === t.itemCode)) problems.push("trial_item_missing:" + t.id);
  }
  // 3. 冻结关系
  for (const f of db.freezes) {
    if (!db.alerts.some(a => a.id === f.alertId)) problems.push("freeze_alert_missing:" + f.id);
    if (f.targetKey !== f.targetType + ":" + f.targetId) problems.push("freeze_key_bad:" + f.id);
  }
  // 4. 活跃预警的冻结必须在；已关闭预警不得留冻结中
  for (const a of db.alerts) {
    const active = a.status !== "已关闭";
    const fz = db.freezes.filter(f => f.alertId === a.id);
    if (active && !fz.some(f => f.status === "冻结中")) problems.push("alert_freeze_missing:" + a.id);
    if (!active && fz.some(f => f.status === "冻结中")) problems.push("closed_alert_still_frozen:" + a.id);
    // 5. 召回快照与复测
    if (a.decision === "召回") {
      if (!a.recallSnapshot) problems.push("recall_snapshot_missing:" + a.id);
      const trialCount = db.trials.filter(t => t.batchId === a.batchId).length;
      const retestCount = db.retests.filter(r => r.alertId === a.id).length;
      if (retestCount < trialCount) problems.push("retest_missing:" + a.id);
    }
  }
  // 6. targetKey 冻结中唯一
  const seen = new Map();
  for (const f of db.freezes.filter(x => x.status === "冻结中")) {
    if (seen.has(f.targetKey)) problems.push("freeze_duplicate_target:" + f.targetKey);
    seen.set(f.targetKey, f.id);
  }
  return {
    ok: problems.length === 0,
    revision: db.revision,
    schemaVersion: db.schemaVersion,
    counts: {
      items: db.items.length, batches: db.batches.length, samples: db.samples.length,
      trials: db.trials.length, alerts: db.alerts.length, freezes: db.freezes.length,
      freezesActive: db.freezes.filter(f => f.status === "冻结中").length,
      retests: db.retests.length, audit: db.audit.length
    },
    problems
  };
}

// 启动前迁移：仅补结构、落盘一次，然后才监听端口，避免与早期写请求竞争
const boot = await loadDb();
if (boot.changed) await saveDbAtomic(boot.db);

server.listen(port, () => {
  console.log("墨锭试磨室（含原料风险追溯台）listening on http://localhost:" + port);
});
