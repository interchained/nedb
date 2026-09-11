// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

fn main() {
    // napi-rs supports MSVC only on Windows. Its setup() calls
    // windows::setup_gnu() whenever CARGO_CFG_TARGET_ENV == "gnu", and that
    // function panics with:
    //
    //     libnode.dll not found in any search path
    //
    // which reads like a missing dependency but is not. There is no correct
    // value for LIBNODE_PATH here — the gnu target is unsupported, not
    // misconfigured. Nothing in this project ships it either: every Windows job
    // in release.yml targets x86_64-pc-windows-msvc, and PyPI rejects
    // mingw_x86_64 platform tags outright.
    //
    // The practical consequence was that anyone in a MINGW64 / Git Bash shell —
    // where cargo defaults to x86_64-pc-windows-gnu — got that panic from a
    // plain `cargo build`, before a single line of engine code compiled. The
    // error names a Node artifact, so it looks like the repo is broken.
    //
    // Fail with an explanation instead of a mystery.
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    if os == "windows" && env == "gnu" {
        panic!(
            "\n\
             \n  nedb-node cannot be built for x86_64-pc-windows-gnu.\
             \n\
             \n  napi-rs requires the MSVC toolchain on Windows. You are almost\
             \n  certainly in a MINGW64 / Git Bash / MSYS2 shell, where cargo\
             \n  defaults to the gnu target.\
             \n\
             \n  Building the ENGINE (this is what you usually want):\
             \n      cargo build --release --features cast\
             \n      -- nedb-node is excluded from default-members, so this\
             \n         skips the Node binding entirely.\
             \n\
             \n  Building the Node addon deliberately:\
             \n      rustup target add x86_64-pc-windows-msvc\
             \n      npx napi build --platform --release \\\
             \n          --target x86_64-pc-windows-msvc --cargo-cwd rust/crates/nedb-node\
             \n\
             \n  Requires Visual Studio Build Tools + the Windows SDK.\
             \n"
        );
    }

    napi_build::setup();
}
