#!/usr/bin/env python3
"""
Extract nedb/_native.* extension files from built wheels into native-ext/nedb/.
Run from repo root.  Works on Linux, macOS, Windows (no unzip required).

Usage:
    python3 scripts/extract_native.py          # default: dist/*.whl -> native-ext/
    python3 scripts/extract_native.py dist/ out/
"""
import glob, os, sys, zipfile

wheel_pattern = sys.argv[1] if len(sys.argv) > 1 else "dist/*.whl"
out_dir = sys.argv[2] if len(sys.argv) > 2 else "native-ext"

wheels = glob.glob(wheel_pattern)
if not wheels:
    print(f"extract_native.py: no wheels found matching {wheel_pattern!r}")
    sys.exit(0)

extracted = []
for whl in wheels:
    with zipfile.ZipFile(whl) as z:
        for name in z.namelist():
            if "_native" in name and name.startswith("nedb/"):
                dest = os.path.join(out_dir, name)
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                z.extract(name, out_dir)
                try:
                    os.chmod(dest, 0o755)
                except Exception:
                    pass
                print(f"  extracted: {dest}")
                extracted.append(dest)

print(f"extract_native.py: {len(extracted)} file(s) extracted from {len(wheels)} wheel(s)")
