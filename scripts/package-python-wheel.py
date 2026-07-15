#!/usr/bin/env python3
import base64
import hashlib
import os
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


TARGETS = [
    ("x86_64-unknown-linux-musl", "musllinux_1_2_x86_64", "dbtool"),
    ("aarch64-unknown-linux-musl", "musllinux_1_2_aarch64", "dbtool"),
    ("x86_64-apple-darwin", "macosx_11_0_x86_64", "dbtool"),
    ("aarch64-apple-darwin", "macosx_11_0_arm64", "dbtool"),
    ("x86_64-pc-windows-msvc", "win_amd64", "dbtool.exe"),
    ("aarch64-pc-windows-msvc", "win_arm64", "dbtool.exe"),
]

PACKAGE_NAME = "dbtool-bin"
DIST_NAME = "dbtool_bin"


def main() -> int:
    if len(sys.argv) != 4:
        print(
            "usage: package-python-wheel.py <artifact-root> <out-dir> <ref-name>",
            file=sys.stderr,
        )
        return 1

    artifact_root = Path(sys.argv[1]).resolve()
    out_dir = Path(sys.argv[2]).resolve()
    version = normalize_version(sys.argv[3])
    repo_root = Path(__file__).resolve().parents[1]
    package_src = repo_root / "dist" / "python" / DIST_NAME
    work_dir = out_dir / ".work"
    cli_artifacts = work_dir / "cli-artifacts"
    selected_targets = select_targets()
    selected_binaries = {
        target: find_binary(artifact_root, target, exe)
        for target, _tag, exe in selected_targets
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.whl"):
        old.unlink()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)
    generate_cli_artifacts(repo_root, artifact_root, cli_artifacts)

    for target, tag, exe in selected_targets:
        build_wheel(
            out_dir,
            package_src,
            cli_artifacts,
            version,
            tag,
            exe,
            selected_binaries[target],
        )

    return 0


def select_targets() -> list[tuple[str, str, str]]:
    requested = os.environ.get("DBTOOL_PACKAGE_TARGETS")
    if not requested:
        return TARGETS
    names = requested.split(",")
    if any(not name for name in names):
        raise ValueError(
            "DBTOOL_PACKAGE_TARGETS must be a comma-separated list without empty entries"
        )
    if len(set(names)) != len(names):
        raise ValueError("DBTOOL_PACKAGE_TARGETS must not contain duplicate targets")
    by_name = {entry[0]: entry for entry in TARGETS}
    selected = []
    for name in names:
        if name not in by_name:
            raise ValueError(f"unsupported DBTOOL_PACKAGE_TARGETS entry: {name}")
        selected.append(by_name[name])
    return selected


def normalize_version(ref_name: str) -> str:
    version = ref_name[1:] if ref_name.startswith("v") else ref_name
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?", version):
        raise ValueError(f"release ref {ref_name} does not look like a package version")
    return version


def find_binary(artifact_root: Path, target: str, exe: str) -> Path:
    candidates = [
        artifact_root / f"dbtool-bin-{target}" / exe,
        artifact_root / target / exe,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"missing Python wheel binary artifact for {target}")


def generate_cli_artifacts(repo_root: Path, artifact_root: Path, out_dir: Path) -> None:
    script = repo_root / "scripts" / "generate-cli-artifacts.sh"
    subprocess.run(
        ["bash", str(script), str(artifact_root), str(out_dir)],
        check=True,
    )


def build_wheel(
    out_dir: Path,
    package_src: Path,
    cli_artifacts: Path,
    version: str,
    tag: str,
    exe: str,
    binary: Path,
) -> None:
    dist_info = f"{DIST_NAME}-{version}.dist-info"
    wheel_name = f"{DIST_NAME}-{version}-py3-none-{tag}.whl"
    wheel_path = out_dir / wheel_name
    files: list[tuple[str, bytes, int]] = []

    init_py = (package_src / "__init__.py").read_text().replace(
        "__version__ = \"0.0.0-development\"",
        f"__version__ = \"{version}\"",
    )
    files.append((f"{DIST_NAME}/__init__.py", init_py.encode(), 0o644))
    files.append((f"{DIST_NAME}/cli.py", (package_src / "cli.py").read_bytes(), 0o644))
    files.append((f"{DIST_NAME}/{exe}", binary.read_bytes(), 0o755))
    for relative in [
        "completions/dbtool.bash",
        "completions/dbtool.zsh",
        "completions/dbtool.fish",
        "man/dbtool.1",
    ]:
        files.append(
            (
                f"{DIST_NAME}/{relative}",
                (cli_artifacts / relative).read_bytes(),
                0o644,
            )
        )

    files.append(
        (
            f"{dist_info}/METADATA",
            metadata(version).encode(),
            0o644,
        )
    )
    files.append((f"{dist_info}/WHEEL", wheel_metadata(tag).encode(), 0o644))
    files.append((f"{dist_info}/entry_points.txt", b"[console_scripts]\ndbtool=dbtool_bin.cli:main\n", 0o644))

    write_wheel(wheel_path, files, f"{dist_info}/RECORD")
    print(f"wrote {wheel_path}")


def metadata(version: str) -> str:
    return f"""Metadata-Version: 2.3
Name: {PACKAGE_NAME}
Version: {version}
Summary: dbtool command-line binary wrapper
Author-email: YoVinchen <gzh298255@gmail.com>
License: MIT OR Apache-2.0
Project-URL: Repository, https://github.com/YoVinchen/db-tool
Requires-Python: >=3.8
Description-Content-Type: text/markdown

dbtool-bin provides the dbtool CLI as a platform wheel for pip and uv installs.
"""


def wheel_metadata(tag: str) -> str:
    return f"""Wheel-Version: 1.0
Generator: dbtool package-python-wheel.py
Root-Is-Purelib: false
Tag: py3-none-{tag}
"""


def write_wheel(
    wheel_path: Path,
    files: list[tuple[str, bytes, int]],
    record_path: str,
) -> None:
    records: list[str] = []
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as wheel:
        for path, data, mode in files:
            info = zipfile.ZipInfo(path)
            info.external_attr = mode << 16
            wheel.writestr(info, data)
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
            records.append(f"{path},sha256={digest},{len(data)}")

        records.append(f"{record_path},,")
        info = zipfile.ZipInfo(record_path)
        info.external_attr = 0o644 << 16
        wheel.writestr(info, ("\n".join(records) + "\n").encode())


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, OSError, subprocess.CalledProcessError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
