import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repoRoot = fileURLToPath(new URL("../..", import.meta.url));
const tempRoot = join(repoRoot, ".tmp");
const goPath = join(tempRoot, "gopath");
const binary = join(
  tempRoot,
  process.platform === "win32" ? "threadmilld-e2e.exe" : "threadmilld-e2e",
);

mkdirSync(tempRoot, { recursive: true });
const result = spawnSync("go", ["build", "-o", binary, "./cmd/threadmilld"], {
  cwd: repoRoot,
  stdio: "inherit",
  env: {
    ...process.env,
    GOPATH: goPath,
    GOMODCACHE: join(goPath, "pkg", "mod"),
    GOCACHE: join(tempRoot, "gocache"),
  },
});

if (result.error) throw result.error;
process.exit(result.status ?? 1);
