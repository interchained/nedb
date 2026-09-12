#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)
#
# Stage everything the maturin native wheel needs into rust/crates/nedb-py/.
#
# maturin only packages files that live INSIDE its project root, and the two
# things this wheel needs both live outside it:
#
#   python/nedb/   the Python source and the nedbd-v2 binary
#   README.md      the project description
#
# The README matters more than it looks. The native wheels are uploaded to
# PyPI BEFORE the universal wheel, and PyPI takes a release's description from
# the FIRST file uploaded — so if this wheel has no readme, the project page
# reads "The author of this package has not provided a project description"
# even when the root pyproject.toml has one.
#
# This lives in a script rather than inline in a workflow because it is needed
# by release.yml, by test.yml, and by anyone running `maturin build` locally.
# It was inline once, in release.yml only, and test.yml promptly failed with
# "Failed to read readme specified in pyproject.toml".
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dest="$root/rust/crates/nedb-py"

rm -rf "$dest/python"
mkdir -p "$dest/python"
cp -r "$root/python/nedb" "$dest/python/nedb"
cp "$root/README.md" "$dest/README.md"

find "$dest/python/nedb" -name "nedbd-v2*" -exec chmod +x {} \; || true

echo "staged $(find "$dest/python/nedb" -name '*.py' | wc -l) .py files + README.md into rust/crates/nedb-py/"
ls "$dest"/python/nedb/nedbd-v2* 2>/dev/null || echo "(no nedbd-v2 binary staged — expected unless CI built one)"
