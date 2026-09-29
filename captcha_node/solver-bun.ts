// solver-bun.ts — Bun 入口的阿里云无痕验证求解器。
// 求解核心为 vendored 上游实现 captcha-happy.ts（源: jarvanh/zcode-api
// src/proxy/captcha-happy.ts @ 4.7.1），本文件只做子进程协议包装：
// 用法: bun solver-bun.ts <scene> <region> <prefix>
// stdout: VERIFY_PARAM=<param>
// 退出码: 0 成功 / 2 超时或失速 / 3 初始化失败 / 4 fail / 5 onError / 6 参数无效
// 与 solver.js（Node 移植版）协议完全一致，可经 ZCODE_CAPTCHA_SOLVER_JS +
// ZCODE_NODE_PATH 切换，回滚只需清掉这两个环境变量。
import { solveTraceless } from "./captcha-happy.js";

const SCENE = process.argv[2] || "11xygtvd";
const REGION = process.argv[3] || "sgp";
const PREFIX = process.argv[4] || "no8xfe";

process.on("uncaughtException", (err) => {
  process.stderr.write(`[guest-uncaught] ${err && err.message ? err.message : String(err)}\n`);
});
process.on("unhandledRejection", (reason) => {
  process.stderr.write(`[guest-unhandledRejection] ${reason && reason.message ? reason.message : String(reason)}\n`);
});

try {
  const param = await solveTraceless({
    scene: SCENE,
    region: REGION,
    prefix: PREFIX,
  });
  process.stdout.write("VERIFY_PARAM=" + param + "\n");
  process.exit(0);
} catch (err) {
  const msg = err && err.message ? err.message : String(err);
  process.stderr.write(`[solve-fail] ${msg}\n`);
  // 退出码分类与 solver.js 对齐
  let code = 4;
  if (/timeout|stall/.test(msg)) code = 2;
  else if (/waitFor timeout/.test(msg)) code = 3;
  else if (/fail:/.test(msg)) code = 4;
  else if (/onError:/.test(msg)) code = 5;
  else if (/verify param|degraded|securityToken|base64/.test(msg)) code = 6;
  process.exit(code);
}
