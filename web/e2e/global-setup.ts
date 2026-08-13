import type { FullConfig } from "@playwright/test";
import { spawn, type ChildProcess } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const readyURL = "http://127.0.0.1:18080/readyz";

function delay(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function isReady() {
  try {
    const response = await fetch(readyURL, {
      signal: AbortSignal.timeout(500),
    });
    return response.ok;
  } catch {
    return false;
  }
}

async function stop(server: ChildProcess) {
  if (server.exitCode !== null) return;
  const exited = new Promise<void>((resolve) =>
    server.once("exit", () => resolve()),
  );
  server.kill(process.platform === "win32" ? undefined : "SIGTERM");
  await Promise.race([exited, delay(2_000)]);
  if (server.exitCode === null) {
    server.kill("SIGKILL");
    await Promise.race([exited, delay(2_000)]);
  }
}

export default async function globalSetup(_config: FullConfig) {
  if (await isReady()) {
    throw new Error(`${readyURL} is already in use; stop the existing host`);
  }

  const webRoot = fileURLToPath(new URL("..", import.meta.url));
  const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
  const binary = join(
    repoRoot,
    ".tmp",
    process.platform === "win32" ? "threadmilld-e2e.exe" : "threadmilld-e2e",
  );
  const server = spawn(
    binary,
    ["serve", "--fake", "--http-addr", "127.0.0.1:18080", "--web-dist", "dist"],
    { cwd: webRoot, stdio: ["ignore", "pipe", "pipe"], windowsHide: true },
  );
  let output = "";
  server.stdout?.on("data", (chunk) => {
    output += chunk.toString();
  });
  server.stderr?.on("data", (chunk) => {
    output += chunk.toString();
  });

  for (let attempt = 0; attempt < 200; attempt++) {
    if (server.exitCode !== null) {
      throw new Error(`fake host exited with ${server.exitCode}: ${output}`);
    }
    if (await isReady()) {
      return async () => stop(server);
    }
    await delay(100);
  }

  await stop(server);
  throw new Error(`fake host did not become ready: ${output}`);
}
