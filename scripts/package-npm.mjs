#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import {
  chmodSync,
  copyFileSync,
  cpSync,
  mkdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

process.on("uncaughtException", (error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`error: ${message}`);
  process.exit(1);
});

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
const cliArtifactsDir = join(workDir, "cli-artifacts");
const version = normalizeVersion(refNameArg);
const selectedTargets = selectTargets(targets, process.env.DBTOOL_PACKAGE_TARGETS);
const selectedBinaries = new Map(
  selectedTargets.map((target) => [target.target, findBinary(target)]),
);

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });
mkdirSync(workDir, { recursive: true });
generateCliArtifacts();

const optionalDependencies = {};
for (const target of selectedTargets) {
  optionalDependencies[target.packageName] = version;
  packPlatformPackage(target);
}

packMainPackage(optionalDependencies);

function selectTargets(available, requested) {
  if (!requested) {
    return available;
  }
  const names = requested.split(",");
  if (names.some((name) => !name)) {
    throw new Error("DBTOOL_PACKAGE_TARGETS must be a comma-separated list without empty entries");
  }
  if (new Set(names).size !== names.length) {
    throw new Error("DBTOOL_PACKAGE_TARGETS must not contain duplicate targets");
  }
  const byName = new Map(available.map((target) => [target.target, target]));
  return names.map((name) => {
    const target = byName.get(name);
    if (!target) {
      throw new Error(`unsupported DBTOOL_PACKAGE_TARGETS entry: ${name}`);
    }
    return target;
  });
}

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
  ];
  const found = candidates.find((candidate) => {
    try {
      return statSync(candidate).isFile();
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
  const packagedBinary = join(dir, "bin", target.exe);
  copyFileSync(selectedBinaries.get(target.target), packagedBinary);
  if (target.os !== "win32") {
    chmodSync(packagedBinary, 0o755);
  }
  copyCliArtifacts(dir);
  writeFileSync(
    join(dir, "package.json"),
    `${JSON.stringify(
      {
        name: target.packageName,
        version,
        description: `dbtool binary for ${target.target}`,
        license: "MIT OR Apache-2.0",
        publishConfig: {
          access: "public",
          registry: "https://registry.npmjs.org/",
        },
        preferUnplugged: true,
        repository: {
          type: "git",
          url: "git+https://github.com/yovinchen/db-tool.git",
        },
        os: [target.os],
        cpu: [target.cpu],
        files: ["bin", "completions", "man", "LICENSE-MIT", "LICENSE-APACHE-2.0"],
      },
      null,
      2,
    )}\n`,
  );
  copyLicenses(dir);
  npmPack(dir);
}

function packMainPackage(optionalDependencies) {
  const src = join(repoRoot, "dist", "npm");
  const dir = join(workDir, "main");
  mkdirSync(join(dir, "bin"), { recursive: true });
  copyFileSync(join(src, "bin", "dbtool.js"), join(dir, "bin", "dbtool.js"));
  copyFileSync(join(src, "README.md"), join(dir, "README.md"));
  copyCliArtifacts(dir);
  copyLicenses(dir);

  const packageJson = JSON.parse(readFileSync(join(src, "package.json"), "utf8"));
  packageJson.version = version;
  packageJson.optionalDependencies = optionalDependencies;
  packageJson.files = Array.from(new Set([...(packageJson.files ?? []), "completions", "man"]));
  writeFileSync(join(dir, "package.json"), `${JSON.stringify(packageJson, null, 2)}\n`);
  npmPack(dir);
}

function generateCliArtifacts() {
  const script = join(repoRoot, "scripts", "generate-cli-artifacts.sh");
  const result = spawnSync("bash", [script, artifactRoot, cliArtifactsDir], {
    stdio: "inherit",
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function copyCliArtifacts(dir) {
  cpSync(join(cliArtifactsDir, "completions"), join(dir, "completions"), { recursive: true });
  cpSync(join(cliArtifactsDir, "man"), join(dir, "man"), { recursive: true });
}

function copyLicenses(dir) {
  for (const name of ["LICENSE-MIT", "LICENSE-APACHE-2.0"]) {
    copyFileSync(join(repoRoot, name), join(dir, name));
  }
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
