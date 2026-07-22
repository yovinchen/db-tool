#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const platforms = {
  "linux-x64": {
    packageName: "@yovinchen/dbtool-linux-x64",
    exe: "dbtool",
  },
  "linux-arm64": {
    packageName: "@yovinchen/dbtool-linux-arm64",
    exe: "dbtool",
  },
  "darwin-x64": {
    packageName: "@yovinchen/dbtool-darwin-x64",
    exe: "dbtool",
  },
  "darwin-arm64": {
    packageName: "@yovinchen/dbtool-darwin-arm64",
    exe: "dbtool",
  },
  "win32-x64": {
    packageName: "@yovinchen/dbtool-win32-x64",
    exe: "dbtool.exe",
  },
  "win32-arm64": {
    packageName: "@yovinchen/dbtool-win32-arm64",
    exe: "dbtool.exe",
  },
};

try {
  const binary = resolveBinary();
  const result = spawnSync(binary, process.argv.slice(2), { stdio: "inherit" });

  if (result.error) {
    throw result.error;
  }

  process.exit(result.status ?? 1);
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`dbtool: ${message}`);
  process.exit(1);
}

function resolveBinary() {
  if (process.env.DBTOOL_BINARY) {
    return process.env.DBTOOL_BINARY;
  }

  const platform = platforms[`${process.platform}-${process.arch}`];
  if (!platform) {
    throw new Error(`unsupported platform: ${process.platform}-${process.arch}`);
  }

  try {
    return require.resolve(`${platform.packageName}/bin/${platform.exe}`);
  } catch {
    const vendored = path.join(__dirname, "..", "vendor", platform.exe);
    if (fs.existsSync(vendored)) {
      return vendored;
    }
    throw new Error(
      `missing ${platform.packageName}; reinstall @yovinchen/dbtool, or set DBTOOL_BINARY=/path/to/dbtool`,
    );
  }
}
