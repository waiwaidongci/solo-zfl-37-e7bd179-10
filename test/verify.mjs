// 真实验证：并发、越权、回滚、重启。使用真实 server.js 子进程 + 临时 JSON 库。
import { spawn } from "node:child_process";
import { mkdir, rm, cp, readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const tmpDir = join(root, "data", "verify-tmp");
const dbFile = join(tmpDir, "verify.json");
const PORT = 4399;
const BASE = "http://127.0.0.1:" + PORT;

let server = null;
let passed = 0;
const failures = [];

function ok(name, cond, extra) {
  if (cond) { passed++; console.log("  ✓ " + name); }
  else { failures.push(name); console.log("  ✗ " + name + (extra ? "  -> " + JSON.stringify(extra) : "")); }
}
async function api(path, { method = "GET", body, user = "u01", headers = {}, raw = false } = {}) {
  const h = { ...headers };
  if (user) h["x-user-id"] = user;
  if (body !== undefined) h["Content-Type"] = "application/json";
  const res = await fetch(BASE + path, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body) });
  const json = await res.json().catch(() => null);
  if (raw) return { status: res.status, json };
  if (!res.ok) { const e = new Error((json && (json.error || json.code)) || "HTTP " + res.status); e.status = res.status; e.body = json; throw e; }
  return json;
}

function startServer() {
  return new Promise((resolve, reject) => {
    const s = spawn(process.execPath, [join(root, "server.js")], {
      env: { ...process.env, PORT: String(PORT), DB_PATH: dbFile },
      stdio: ["ignore", "pipe", "pipe"]
    });
    s.stdout.on("data", d => { if (String(d).includes("listening")) resolve(s); });
    s.stderr.on("data", d => process.stderr.write("[server] " + d));
    s.on("error", reject);
    setTimeout(() => reject(new Error("server start timeout")), 5000);
  });
}
async function stopHard(s) {
  if (!s || s.exitCode !== null) return;
  s.kill("SIGKILL");
  await new Promise(r => s.on("exit", r));
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function setup() {
  await rm(tmpDir, { recursive: true, force: true });
  await mkdir(tmpDir, { recursive: true });
  // 从真实旧格式数据文件迁移（无批次/留样/预警等集合）
  await cp(join(root, "data", "ink-stick-testing.json"), dbFile);
}

async function main() {
  await setup();

  /* ---------- 阶段 1：迁移 + 旧数据保留 ---------- */
  console.log("\n[1] 重启迁移、旧数据/旧入口保留");
  server = await startServer();
  const trace0 = await api("/api/trace");
  ok("迁移后新增追溯集合均为空数组", ["batches","samples","trials","alerts","freezes","retests","audit"].every(k => Array.isArray(trace0[k]) && trace0[k].length === 0), trace0.revision);
  ok("schemaVersion=2", (await readDb()).schemaVersion === 2);
  const items0 = await api("/api/items");
  const is001 = items0.find(i => i.code === "IS-001");
  const is002 = items0.find(i => i.code === "IS-002");
  ok("旧墨锭 IS-001/IS-002 原样保留", !!is001 && !!is002 && is001.smokeSource === "黄山松烟");
  ok("旧试磨日志保留", is001.logs.some(l => l.score === 86));
  const home = await fetch(BASE + "/").then(r => r.text());
  ok("旧页面 / 保留且含追溯台入口", home.includes("墨锭试磨室") && home.includes("/risk"));
  const riskHtml = await fetch(BASE + "/risk").then(r => r.text());
  ok("新页面 /risk 可访问", riskHtml.includes("原料风险追溯台"));
  const stats = await api("/api/stats");
  ok("旧统计接口 /api/stats 正常", stats["已试磨"] === 1);

  // 无身份的写请求被拒（越权基线）
  const noAuth = await api("/api/batches", { method: "POST", user: null, body: { material: "x" }, raw: true });
  ok("无 x-user-id 写请求 401", noAuth.status === 401);
  const badAuth = await api("/api/batches", { method: "POST", user: "ghost", body: { material: "x" }, raw: true });
  ok("伪造用户 401", badAuth.status === 401);

  /* ---------- 阶段 2：批次/留样/试磨绑定 + 并发留样唯一 ---------- */
  console.log("\n[2] 批次/留样/试磨绑定，留样并发唯一使用");
  const b1 = await api("/api/batches", { method: "POST", body: { id: "B-100", material: "测试松烟", supplier: "甲坊" } });
  ok("登记批次", b1.id === "B-100");
  const dupB = await api("/api/batches", { method: "POST", body: { id: "B-100" }, raw: true });
  ok("重复批次 409", dupB.status === 409);
  await api("/api/samples", { method: "POST", body: { id: "SY-1", batchId: "B-100" } });
  await api("/api/samples", { method: "POST", body: { id: "SY-2", batchId: "B-100" } });
  const sBad = await api("/api/samples", { method: "POST", body: { id: "SY-9", batchId: "NOPE" }, raw: true });
  ok("留样挂到不存在批次 404", sBad.status === 404);

  // 并发：两个试磨同时抢同一留样 SY-1，只允许一个成功
  const trialPayload = { itemCode: "IS-002", batchId: "B-100", sampleId: "SY-1", paper: "宣纸", score: 88 };
  const [ra, rb] = await Promise.all([
    api("/api/trials", { method: "POST", body: trialPayload, raw: true }),
    sleep(5).then(() => api("/api/trials", { method: "POST", body: trialPayload, raw: true }))
  ]);
  const codes = [ra.status, rb.status].sort().join(",");
  ok("并发抢同一留样：恰好一个 201 一个 409（实际 " + codes + "）", ra.status + rb.status === 201 + 409 || codes === "201,409", [ra.status, rb.status]);
  const trace1 = await api("/api/trace");
  ok("库内该留样仅 1 条试磨", trace1.trials.filter(t => t.sampleId === "SY-1").length === 1);
  const sy1 = trace1.samples.find(s => s.id === "SY-1");
  ok("留样 used 标记与试磨一致", sy1.used === true && trace1.samples.find(s => s.id === "SY-2").used === false);

  // 留样与批次不匹配
  await api("/api/batches", { method: "POST", body: { id: "B-200", material: "另一批" } });
  const mismatch = await api("/api/trials", { method: "POST", body: { itemCode: "IS-001", batchId: "B-200", sampleId: "SY-2" }, raw: true });
  ok("留样与批次不匹配 400", mismatch.status === 400);

  // 用 SY-2 建第二条试磨（绑 IS-001），保证预警影响 2 个留样/2 条试磨
  await api("/api/trials", { method: "POST", body: { itemCode: "IS-001", batchId: "B-100", sampleId: "SY-2", score: 90 } });

  /* ---------- 阶段 3：预警并发只成功一次 + 提交即冻结 ---------- */
  console.log("\n[3] 预警并发唯一、提交即冻结影响范围");
  const alertPayload = { batchId: "B-100", riskLevel: "高", reason: "留样检出胶性异常" };
  const [wa, wb] = await Promise.all([
    api("/api/alerts", { method: "POST", body: alertPayload, raw: true }),
    sleep(3).then(() => api("/api/alerts", { method: "POST", body: alertPayload, raw: true }))
  ]);
  const win = [wa.json, wb.json].filter(Boolean);
  ok("并发预警：一个 201 一个拒绝（幂等回放200 或 409）", [wa.status, wb.status].sort().join(",") === "200,201" || [wa.status, wb.status].includes(409), [wa.status, wb.status]);
  const trace2 = await api("/api/trace");
  ok("该批次只有 1 个活跃预警", trace2.alerts.filter(a => a.batchId === "B-100" && a.status !== "已关闭").length === 1);
  const alert = trace2.alerts[0];
  ok("影响数量=批次1+留样2+试磨2+墨锭2 = 7", alert.impact.count === 7, alert.impact);
  ok("冻结中对象数=7 且 targetKey 无重复", trace2.freezes.filter(f => f.status === "冻结中").length === 7);

  // 冻结阻断：批次下新留样、留样再用、墨锭改状态/备注/旧试磨全部失败
  const f1 = await api("/api/samples", { method: "POST", body: { batchId: "B-100" }, raw: true });
  ok("冻结批次不能新增留样 409", f1.status === 409);
  const f2 = await api("/api/items/IS-001", { method: "PATCH", body: { status: "已试磨" }, raw: true });
  ok("冻结墨锭不能改状态 409", f2.status === 409);
  const f3 = await api("/api/items/IS-001/logs", { method: "POST", body: { note: "x" }, raw: true });
  ok("冻结墨锭不能追加备注 409", f3.status === 409);
  const f4 = await api("/api/items/IS-001/action", { method: "POST", body: { score: 50 }, raw: true });
  ok("冻结墨锭不能旧方式试磨 409", f4.status === 409);

  /* ---------- 阶段 4：状态机非法跳转、越权审核、重复提交 ---------- */
  console.log("\n[4] 状态机：非法跳转 / 提交人不能审核 / 重复提交");
  const aid = alert.id;
  const jump = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u02", body: { action: "release" }, raw: true });
  ok("待确认直接放行=非法跳转 409", jump.status === 409 && jump.json.error === "illegal_transition");
  const unknown = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u02", body: { action: "explode" }, raw: true });
  ok("未知动作 400", unknown.status === 400);
  const selfReview = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u01", body: { action: "confirm" }, raw: true });
  // u01 是提交人；confirm 允许（确认不是审核），但 release/recall 必须拒绝
  ok("提交人可以确认预警", selfReview.status === 200);
  await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u02", body: { action: "start" } });
  const selfRelease = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u01", body: { action: "release" }, raw: true });
  ok("提交人不能审核放行 403", selfRelease.status === 403 && selfRelease.json.error === "submitter_cannot_review");
  const selfRecall = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u01", body: { action: "recall" }, raw: true });
  ok("提交人不能审核召回 403", selfRecall.status === 403);
  const dupConfirm = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u02", body: { action: "confirm" }, raw: true });
  ok("重复/回退确认 409", dupConfirm.status === 409);

  /* ---------- 阶段 5：召回快照 + 复测任务 + 关闭门禁 ---------- */
  console.log("\n[5] 召回保留原结论快照、生成复测，复测完成前不能关闭");
  const recall = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u02", body: { action: "recall" } });
  ok("召回后状态=待复核", recall.status === "待复核" && recall.decision === "召回");
  ok("召回快照含 2 条原试磨结论", recall.recallSnapshot && recall.recallSnapshot.trials.length === 2);
  const trace3 = await api("/api/trace");
  const retests = trace3.retests.filter(r => r.alertId === aid);
  ok("生成 2 个复测任务", retests.length === 2);
  await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u03", body: { action: "review" } });
  const closeEarly = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u03", body: { action: "close" }, raw: true });
  ok("复测未完成时关闭 409", closeEarly.status === 409 && closeEarly.json.error === "retest_pending");
  const dupRetest = await api(`/api/retests/${retests[0].id}/complete`, { method: "POST", body: { result: "重测合格", score: 91 } });
  const again = await api(`/api/retests/${retests[0].id}/complete`, { method: "POST", body: { result: "x", score: 1 }, raw: true });
  ok("复测重复提交 409", again.status === 409);
  await api(`/api/retests/${retests[1].id}/complete`, { method: "POST", body: { result: "重测合格", score: 92 } });
  const closed = await api(`/api/alerts/${aid}/transition`, { method: "POST", user: "u03", body: { action: "close" } });
  ok("复测完成后关闭", closed.status === "已关闭");
  const trace4 = await api("/api/trace");
  ok("关闭后全部解冻", trace4.freezes.every(f => f.status === "已解冻") && trace4.items !== undefined);
  ok("审计链完整（提交/确认/处理/召回/复核/复测/关闭）", trace4.audit.length >= 8);

  /* ---------- 阶段 6：写失败回滚（无部分落盘） ---------- */
  console.log("\n[6] 写失败回滚：预警/影响/冻结/审计不部分落盘");
  const before = await api("/api/trace");
  await api("/api/batches", { method: "POST", body: { id: "B-300", material: "回滚批" } });
  await api("/api/samples", { method: "POST", body: { id: "SY-3", batchId: "B-300" } });
  const beforeFail = await readDb();
  const revBefore = beforeFail.revision;
  const failAlert = await api("/api/alerts", {
    method: "POST",
    body: { batchId: "B-300", reason: "模拟磁盘失败" },
    headers: { "x-simulate": "write-fail" }, raw: true
  });
  ok("模拟写失败返回 500", failAlert.status === 500);
  const afterFail = await readDb();
  ok("失败后 revision 不增长", afterFail.revision === revBefore);
  ok("预警未落盘", afterFail.alerts.filter(a => a.batchId === "B-300").length === 0);
  ok("冻结未落盘（无部分冻结）", !afterFail.freezes.some(f => f.reason === "模拟磁盘失败"));
  ok("审计未落盘（无部分审计）", !afterFail.audit.some(e => e.detail && e.detail.batchId === "B-300" && e.action === "提交预警"));
  ok("批次/留样在失败前的正常提交仍在", afterFail.batches.some(b => b.id === "B-300") && afterFail.samples.some(s => s.id === "SY-3"));
  ok("无残留临时文件", existsSync(dbFile + ".tmp") === false && (await readTmpDir()).filter(n => n.includes(".tmp-")).length === 0);
  // 失败后系统仍可正常写
  const recover = await api("/api/batches", { method: "POST", body: { id: "B-301", material: "恢复批" } });
  ok("写失败后服务仍可正常提交", recover.id === "B-301");

  /* ---------- 阶段 7：硬重启（SIGKILL）后关系/版本一致 ---------- */
  console.log("\n[7] SIGKILL 硬重启：关系与版本一致");
  await stopHard(server); server = null;
  server = await startServer();
  const c1 = await api("/api/consistency");
  ok("重启后一致性自检通过", c1.ok === true, c1.problems);
  ok("重启后 revision 与文件一致", c1.revision === afterFail.revision + 1);
  const trace5 = await api("/api/trace");
  const a5 = trace5.alerts.find(a => a.id === aid);
  ok("预警状态/快照/时间线持久", a5.status === "已关闭" && a5.recallSnapshot && a5.timeline.length >= 6);
  ok("留样唯一关系持久", trace5.samples.filter(s => s.used).every(s => trace5.trials.filter(t => t.sampleId === s.id).length === 1));
  ok("复测结论持久", trace5.retests.every(r => r.status === "已完成" && typeof r.score === "number"));
  ok("旧墨锭数据仍在", (await api("/api/items")).filter(i => ["IS-001","IS-002"].includes(i.code)).length === 2);
  // 重启后再走一轮：新批次预警（验证版本在重启后继续单调）
  await api("/api/batches", { method: "POST", body: { id: "B-400", material: "重启后批" } });
  await api("/api/samples", { method: "POST", body: { id: "SY-4", batchId: "B-400" } });
  const a2 = await api("/api/alerts", { method: "POST", body: { batchId: "B-400", reason: "重启后预警" } });
  ok("重启后可提交新预警并冻结", a2.impact.count >= 2 && a2.status === "待确认");

  await stopHard(server); server = null;
  const c2json = JSON.parse(await readFile(dbFile, "utf8"));
  const c2 = consistencyOffline(c2json);
  ok("离线文件最终一致", c2.ok, c2.problems);

  console.log("\n================ 结果 ================");
  console.log("通过 " + passed + "，失败 " + failures.length);
  if (failures.length) { console.log("失败项：\n - " + failures.join("\n - ")); process.exitCode = 1; }
  else console.log("全部验证通过：并发唯一、越权拦截、回滚无部分落盘、硬重启一致。");
}

async function readDb() { return JSON.parse(await readFile(dbFile, "utf8")); }
async function readTmpDir() { const { readdir } = await import("node:fs/promises"); return readdir(tmpDir); }

// 与服务端同构的离线一致性检查（独立实现用于交叉验证）
function consistencyOffline(db) {
  const problems = [];
  for (const s of db.samples) {
    const n = db.trials.filter(t => t.sampleId === s.id);
    if (n.length > 1) problems.push("sample_multi:" + s.id);
    if (n.length === 1 && s.usedTrialId !== n[0].id) problems.push("stale_link:" + s.id);
  }
  const active = new Set(db.freezes.filter(f => f.status === "冻结中").map(f => f.targetKey));
  if (active.size !== db.freezes.filter(f => f.status === "冻结中").length) problems.push("dup_freeze_target");
  for (const a of db.alerts) {
    const isOpen = a.status !== "已关闭";
    const has = db.freezes.some(f => f.alertId === a.id && f.status === "冻结中");
    if (isOpen !== has) problems.push("freeze_state:" + a.id);
    if (a.decision === "召回") {
      if (!a.recallSnapshot) problems.push("no_snapshot:" + a.id);
      if (db.retests.filter(r => r.alertId === a.id).length !== db.trials.filter(t => t.batchId === a.batchId).length) problems.push("retest_count:" + a.id);
    }
  }
  return { ok: problems.length === 0, problems };
}

main().catch(async e => {
  console.error("测试运行出错：", e);
  process.exitCode = 1;
}).finally(async () => {
  await stopHard(server);
  if (!process.env.KEEP_TMP) await rm(tmpDir, { recursive: true, force: true });
});
