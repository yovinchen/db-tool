import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    binary = os.environ.get("DBTOOL_BINARY")
    if binary is None:
        exe = "dbtool.exe" if os.name == "nt" else "dbtool"
        binary = str(Path(__file__).with_name(exe))

    if not Path(binary).is_file():
        print(
            "dbtool binary is missing; reinstall dbtool-bin or set DBTOOL_BINARY",
            file=sys.stderr,
        )
        return 1

    if os.name == "nt":
        return subprocess.call([binary, *sys.argv[1:]])

    try:
        current_mode = Path(binary).stat().st_mode
        Path(binary).chmod(current_mode | 0o111)
    except OSError:
        pass

    os.execv(binary, [binary, *sys.argv[1:]])
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
