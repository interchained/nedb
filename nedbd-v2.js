#!/usr/bin/env node
// nedbd-v2 — thin platform shim that locates and spawns the right
// pre-built native binary for the current platform/arch.
//
// The actual native binaries are shipped alongside this file in the
// npm package root (same directory as package.json). Naming convention
// matches the .node files produced by napi-rs:
//
//   Linux x64    -> nedbd-v2-linux-x64
//   Windows x64  -> nedbd-v2-win-x64.exe
//   macOS arm64  -> nedbd-v2-darwin-arm64
//   macOS x64    -> nedbd-v2-darwin-x64
//
// We pass through stdin/stdout/stderr and exit with the child's exit code so
// `npx nedbd-v2 …` is indistinguishable from invoking the binary directly.

"use strict";

const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

function resolveBinaryName() {
  const platform = process.platform; // 'linux' | 'darwin' | 'win32' | ...
  const arch = process.arch;         // 'x64' | 'arm64' | ...

  if (platform === "linux" && arch === "x64") {
    return "nedbd-v2-linux-x64";
  }
  if (platform === "win32" && arch === "x64") {
    return "nedbd-v2-win-x64.exe";
  }
  if (platform === "darwin" && arch === "arm64") {
    return "nedbd-v2-darwin-arm64";
  }
  if (platform === "darwin" && arch === "x64") {
    return "nedbd-v2-darwin-x64";
  }
  return null;
}

function main() {
  const name = resolveBinaryName();
  if (!name) {
    process.stderr.write(
      `nedbd-v2: unsupported platform/arch: ${process.platform}/${process.arch}\n` +
        `Supported: linux-x64, win32-x64, darwin-arm64, darwin-x64\n`
    );
    process.exit(1);
  }

  const binPath = path.join(__dirname, name);
  if (!fs.existsSync(binPath)) {
    process.stderr.write(
      `nedbd-v2: binary not found for this platform: ${binPath}\n` +
        `The nedb-engine npm package may be missing the prebuilt binary for ` +
        `${process.platform}/${process.arch}.\n`
    );
    process.exit(1);
  }

  // Ensure executable bit on POSIX (npm sometimes strips it from tarballs).
  if (process.platform !== "win32") {
    try {
      fs.chmodSync(binPath, 0o755);
    } catch (_err) {
      // best-effort; ignore — spawn will surface a clearer error if needed.
    }
  }

  const child = spawn(binPath, process.argv.slice(2), {
    stdio: "inherit",
    windowsHide: false,
  });

  child.on("error", (err) => {
    process.stderr.write(`nedbd-v2: failed to spawn ${binPath}: ${err.message}\n`);
    process.exit(1);
  });

  child.on("exit", (code, signal) => {
    if (signal) {
      // Re-raise the signal so shells see the correct termination cause.
      process.kill(process.pid, signal);
      return;
    }
    process.exit(code === null ? 1 : code);
  });
}

main();
