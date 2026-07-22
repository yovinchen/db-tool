#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const targets = [
  ["x86_64-unknown-linux-musl", "@yovinchen/dbtool-linux-x64", "linux", "x64", "dbtool"],
  ["aarch64-unknown-linux-musl", "@yovinchen/dbtool-linux-arm64", "linux", "arm64", "dbtool"],
  ["x86_64-apple-darwin", "@yovinchen/dbtool-darwin-x64", "darwin", "x64", "dbtool"],
  ["aarch64-apple-darwin", "@yovinchen/dbtool-darwin-arm64", "darwin", "arm64", "dbtool"],
  ["x86_64-pc-windows-msvc", "@yovinchen/dbtool-win32-x64", "win32", "x64", "dbtool.exe"],
  ["aarch64-pc-windows-msvc", "@yovinchen/dbtool-win32-arm64", "win32", "arm64", "dbtool.exe"],
].map(([target, packageName, os, cpu, exe]) => ({ target, packageName, os, cpu, exe }));

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..");
const packageScript = join(scriptDir, "package-npm.mjs");
const wrapperSource = join(repoRoot, "dist", "npm", "bin", "dbtool.js");
const fixtureRoot = mkdtempSync(join(tmpdir(), "dbtool-npm-test-"));
const artifactRoot = join(fixtureRoot, "artifacts");
const outDir = join(fixtureRoot, "packages");
const npmCache = join(fixtureRoot, "npm-cache");
const version = "9.8.7-test.1";
const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";

try {
  const host = targets.find(
    (target) => target.os === process.platform && target.cpu === process.arch,
  );
  assert(host, `test host ${process.platform}-${process.arch} is not in the package matrix`);

  createArtifactMatrix(artifactRoot, host);
  const packaged = run(process.execPath, [packageScript, artifactRoot, outDir, `v${version}`], {
    env: npmEnv(),
  });
  assertSuccess(packaged, "six-platform npm packaging");

  verifyPlatformMappingAndMetadata();
  verifyTarballTopology();
  verifyWrapperFailures(host);
  verifyBinaryOverride(host);
  verifyOfflineInstall(host);
  verifyDryRunPublication();
  verifyFullMatrixFailsClosed(host);

  console.log("npm packaging tests passed: 6 platform packages, wrapper, offline install, dry-run publish");
} finally {
  rmSync(fixtureRoot, { recursive: true, force: true });
}

function createArtifactMatrix(root, host) {
  for (const target of targets) {
    const dir = join(root, `dbtool-bin-${target.target}`);
    mkdirSync(dir, { recursive: true });
    const binary = join(dir, target.exe);
    if (target.target === host.target && process.platform !== "win32") {
      writeFileSync(
        binary,
        `#!/usr/bin/env sh
set -eu
if [ "\${1:-}" = "generate-artifacts" ]; then
  shift
  [ "\${1:-}" = "--out-dir" ]
  out="\${2:?missing artifact output}"
  mkdir -p "$out/completions" "$out/man"
  printf 'complete -F _dbtool dbtool\\n' > "$out/completions/dbtool.bash"
  printf '#compdef dbtool\\n' > "$out/completions/dbtool.zsh"
  printf 'complete -c dbtool\\n' > "$out/completions/dbtool.fish"
  printf '.TH DBTOOL 1\\n' > "$out/man/dbtool.1"
  exit 0
fi
printf 'dbtool ${version}\\n'
`,
      );
      chmodSync(binary, 0o755);
    } else {
      writeFileSync(binary, `fixture for ${target.target}\n`);
    }
  }
}

function verifyPlatformMappingAndMetadata() {
  const source = readFileSync(wrapperSource, "utf8");
  const main = readJson(join(outDir, ".work", "main", "package.json"));
  verifyCommonMetadata(main);
  assert(main.preferUnplugged === true, "main wrapper must prefer an unpacked install");
  assert(Object.keys(main.optionalDependencies).length === targets.length, "main package must map all targets");

  for (const target of targets) {
    const dir = join(outDir, ".work", target.target);
    const metadata = readJson(join(dir, "package.json"));
    assert(metadata.name === target.packageName, `${target.target} package name mismatch`);
    assert(metadata.os?.length === 1 && metadata.os[0] === target.os, `${target.target} OS mismatch`);
    assert(metadata.cpu?.length === 1 && metadata.cpu[0] === target.cpu, `${target.target} CPU mismatch`);
    assert(metadata.preferUnplugged === true, `${target.target} must prefer an unpacked install`);
    assert(main.optionalDependencies[target.packageName] === version, `${target.target} optional dependency missing`);
    assert(source.includes(`"${target.os}-${target.cpu}"`), `${target.target} wrapper platform mapping missing`);
    assert(source.includes(`packageName: "${target.packageName}"`), `${target.target} wrapper package mapping missing`);
    verifyCommonMetadata(metadata);
    verifyLicenseFiles(dir);
    assertFile(join(dir, "bin", target.exe));
  }
  verifyLicenseFiles(join(outDir, ".work", "main"));
}

function verifyCommonMetadata(metadata) {
  assert(metadata.license === "MIT OR Apache-2.0", `${metadata.name} SPDX expression mismatch`);
  assert(
    metadata.repository?.url === "git+https://github.com/yovinchen/db-tool.git",
    `${metadata.name} must use the canonical lowercase repository URL`,
  );
  assert(metadata.publishConfig?.access === "public", `${metadata.name} must publish with public access`);
  assert(
    metadata.publishConfig?.registry === "https://registry.npmjs.org/",
    `${metadata.name} must publish only to the npm registry`,
  );
  assert(metadata.files.includes("LICENSE-MIT"), `${metadata.name} omits LICENSE-MIT`);
  assert(metadata.files.includes("LICENSE-APACHE-2.0"), `${metadata.name} omits LICENSE-APACHE-2.0`);
}

function verifyLicenseFiles(dir) {
  for (const name of ["LICENSE-MIT", "LICENSE-APACHE-2.0"]) {
    const expected = readFileSync(join(repoRoot, name), "utf8");
    const actual = readFileSync(join(dir, name), "utf8");
    assert(actual === expected, `${dir} contains a modified ${name}`);
  }
}

function verifyTarballTopology() {
  const tarballs = readdirSync(outDir).filter((name) => name.endsWith(".tgz"));
  assert(tarballs.length === targets.length + 1, "npm packaging must emit six platform tarballs and one main tarball");
  for (const name of tarballs) {
    const listing = run("tar", ["-tzf", join(outDir, name)]);
    assertSuccess(listing, `inspect ${name}`);
    assert(listing.stdout.includes("package/LICENSE-MIT"), `${name} omits LICENSE-MIT`);
    assert(listing.stdout.includes("package/LICENSE-APACHE-2.0"), `${name} omits LICENSE-APACHE-2.0`);
    assert(listing.stdout.includes("package/package.json"), `${name} omits package.json`);
  }
}

function verifyWrapperFailures(host) {
  const result = run(process.execPath, [join(outDir, ".work", "main", "bin", "dbtool.js"), "--version"]);
  assert(result.status === 1, "wrapper without its host package must fail");
  assert(result.stderr.includes(`dbtool: missing ${host.packageName}`), "missing-package error must name the host package");
  assert(result.stderr.includes("DBTOOL_BINARY=/path/to/dbtool"), "missing-package error must explain the override");
  assert(!result.stderr.includes("    at "), "missing-package error must not print a Node stack");
  assert(result.stderr.trim().split(/\r?\n/).length === 1, "missing-package error must stay on one line");
}

function verifyBinaryOverride(host) {
  if (process.platform === "win32") {
    return;
  }
  const binary = join(artifactRoot, `dbtool-bin-${host.target}`, host.exe);
  const result = run(
    process.execPath,
    [join(outDir, ".work", "main", "bin", "dbtool.js"), "--version"],
    { env: { ...process.env, DBTOOL_BINARY: binary } },
  );
  assertSuccess(result, "DBTOOL_BINARY wrapper override");
  assert(result.stdout.trim() === `dbtool ${version}`, "DBTOOL_BINARY must execute the selected binary");
}

function verifyOfflineInstall(host) {
  if (process.platform === "win32") {
    return;
  }
  const installDir = join(fixtureRoot, "offline-install");
  mkdirSync(installDir, { recursive: true });
  const mainTarball = findTarball(`yovinchen-dbtool-${version}.tgz`);
  const platformTarball = findTarball(
    `${host.packageName.replace("@", "").replace("/", "-")}-${version}.tgz`,
  );
  writeFileSync(
    join(installDir, "package.json"),
    `${JSON.stringify(
      {
        private: true,
        dependencies: {
          "@yovinchen/dbtool": `file:${join(outDir, mainTarball)}`,
          [host.packageName]: `file:${join(outDir, platformTarball)}`,
        },
      },
      null,
      2,
    )}\n`,
  );
  const install = run(
    npmCommand,
    ["install", "--offline", "--ignore-scripts", "--no-audit", "--no-fund"],
    { cwd: installDir, env: npmEnv() },
  );
  assertSuccess(install, "offline host npm install");
  const execute = run(
    process.execPath,
    [join(installDir, "node_modules", "@yovinchen", "dbtool", "bin", "dbtool.js"), "--version"],
    { cwd: installDir },
  );
  assertSuccess(execute, "offline installed wrapper execution");
  assert(execute.stdout.trim() === `dbtool ${version}`, "offline install must dispatch to the host package");
}

function verifyDryRunPublication() {
  const packageDirs = [
    join(outDir, ".work", "main"),
    ...targets.map((target) => join(outDir, ".work", target.target)),
  ];
  for (const dir of packageDirs) {
    const result = run(
      npmCommand,
      ["publish", "--dry-run", "--ignore-scripts", "--json"],
      { cwd: dir, env: npmEnv() },
    );
    assertSuccess(result, `npm publish --dry-run for ${readJson(join(dir, "package.json")).name}`);
  }
}

function verifyFullMatrixFailsClosed(host) {
  const incompleteRoot = join(fixtureRoot, "incomplete-artifacts");
  const incompleteOut = join(fixtureRoot, "incomplete-output");
  mkdirSync(incompleteRoot, { recursive: true });
  mkdirSync(incompleteOut, { recursive: true });
  writeFileSync(join(incompleteOut, "sentinel"), "keep\n");
  const hostSource = join(artifactRoot, `dbtool-bin-${host.target}`, host.exe);
  const hostDir = join(incompleteRoot, `dbtool-bin-${host.target}`);
  mkdirSync(hostDir, { recursive: true });
  writeFileSync(join(hostDir, host.exe), readFileSync(hostSource));
  if (process.platform !== "win32") {
    chmodSync(join(hostDir, host.exe), 0o755);
  }

  const result = run(process.execPath, [packageScript, incompleteRoot, incompleteOut, `v${version}`], {
    env: npmEnv({ DBTOOL_PACKAGE_TARGETS: undefined }),
  });
  assert(result.status !== 0, "default npm packaging must reject an incomplete six-platform matrix");
  assert(result.stderr.includes("missing npm binary artifact for"), "incomplete matrix failure must identify the missing target");
  assertFile(join(incompleteOut, "sentinel"));
}

function findTarball(expected) {
  const matches = readdirSync(outDir).filter((name) => name === expected);
  assert(matches.length === 1, `expected tarball ${expected}, found ${matches.length}`);
  return matches[0];
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function assertFile(path) {
  readFileSync(path);
}

function npmEnv(overrides = {}) {
  const env = {
    ...process.env,
    npm_config_cache: npmCache,
    npm_config_audit: "false",
    npm_config_fund: "false",
    npm_config_update_notifier: "false",
    ...overrides,
  };
  for (const [name, value] of Object.entries(env)) {
    if (value === undefined) {
      delete env[name];
    }
  }
  return env;
}

function run(command, args, options = {}) {
  return spawnSync(command, args, {
    cwd: options.cwd ?? repoRoot,
    env: options.env ?? process.env,
    encoding: "utf8",
  });
}

function assertSuccess(result, label) {
  assert(
    result.status === 0,
    `${label} failed (${result.status ?? "no status"}): ${result.stderr || result.stdout}`,
  );
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}
