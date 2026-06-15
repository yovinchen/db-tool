#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const targets = [
  {
    target: "x86_64-unknown-linux-musl",
    packageName: "@yovinchen/dbtool-linux-x64",
    os: "linux",
    cpu: "x64",
    exe: "dbtool",
  },
  {
    target: "aarch64-unknown-linux-musl",
    packageName: "@yovinchen/dbtool-linux-arm64",
    os: "linux",
    cpu: "arm64",
    exe: "dbtool",
  },
  {
    target: "x86_64-apple-darwin",
    packageName: "@yovinchen/dbtool-darwin-x64",
    os: "darwin",
    cpu: "x64",
    exe: "dbtool",
  },
  {
    target: "aarch64-apple-darwin",
    packageName: "@yovinchen/dbtool-darwin-arm64",
    os: "darwin",
    cpu: "arm64",
    exe: "dbtool",
  },
  {
    target: "x86_64-pc-windows-msvc",
    packageName: "@yovinchen/dbtool-win32-x64",
    os: "win32",
    cpu: "x64",
    exe: "dbtool.exe",
  },
  {
    target: "aarch64-pc-windows-msvc",
    packageName: "@yovinchen/dbtool-win32-arm64",
    os: "win32",
    cpu: "arm64",
    exe: "dbtool.exe",
  },
];

const [artifactRootArg, outDirArg, refNameArg] = process.argv.slice(2);
if (!artifactRootArg || !outDirArg || !refNameArg) {
  console.error("usage: package-npm.mjs <artifact-root> <out-dir> <ref-name>");
  process.exit(1);
}

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");
const artifactRoot = resolve(artifactRootArg);
const outDir = resolve(outDirArg);
const workDir = join(outDir, ".work");
const version = normalizeVersion(refNameArg);

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });
mkdirSync(workDir, { recursive: true });

const optionalDependencies = {};
for (const target of targets) {
  optionalDependencies[target.packageName] = version;
  packPlatformPackage(target);
}

packMainPackage(optionalDependencies);

function normalizeVersion(refName) {
  const version = refName.replace(/^v/, "");
  if (!/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(`release ref ${refName} does not look like a package version`);
  }
  return version;
}

function findBinary(target) {
  const candidates = [
    join(artifactRoot, `dbtool-bin-${target.target}`, target.exe),
    join(artifactRoot, target.target, target.exe),
    join(artifactRoot, target.exe),
  ];
  const found = candidates.find((candidate) => {
    try {
      return readFileSync(candidate).length >= 0;
    } catch {
      return false;
    }
  });
  if (!found) {
    throw new Error(`missing npm binary artifact for ${target.target}`);
  }
  return found;
}

function packPlatformPackage(target) {
  const dir = join(workDir, target.target);
  mkdirSync(join(dir, "bin"), { recursive: true });
  copyFileSync(findBinary(target), join(dir, "bin", target.exe));
  writeFileSync(
    join(dir, "package.json"),
    `${JSON.stringify(
      {
        name: target.packageName,
        version,
        description: `dbtool binary for ${target.target}`,
        license: "MIT OR Apache-2.0",
        repository: {
          type: "git",
          url: "git+https://github.com/YoVinchen/db-tool.git",
        },
        os: [target.os],
        cpu: [target.cpu],
        files: ["bin"],
      },
      null,
      2,
    )}\n`,
  );
  npmPack(dir);
}

function packMainPackage(optionalDependencies) {
  const src = join(repoRoot, "dist", "npm");
  const dir = join(workDir, "main");
  mkdirSync(join(dir, "bin"), { recursive: true });
  copyFileSync(join(src, "bin", "dbtool.js"), join(dir, "bin", "dbtool.js"));
  copyFileSync(join(src, "README.md"), join(dir, "README.md"));

  const packageJson = JSON.parse(readFileSync(join(src, "package.json"), "utf8"));
  packageJson.version = version;
  packageJson.optionalDependencies = optionalDependencies;
  writeFileSync(join(dir, "package.json"), `${JSON.stringify(packageJson, null, 2)}\n`);
  npmPack(dir);
}

function npmPack(dir) {
  const result = spawnSync("npm", ["pack", dir, "--pack-destination", outDir], {
    env: {
      ...process.env,
      npm_config_cache: process.env.npm_config_cache ?? join(workDir, ".npm-cache"),
    },
    stdio: "inherit",
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}
