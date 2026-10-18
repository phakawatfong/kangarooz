"""Browse the lake in the DuckDB UI (http://localhost:4213) while the stream runs.

    uv run python -m lakehouse.ui
"""

import threading

from .lake import LakeConfig, connect


def main() -> None:
    con = connect(LakeConfig.from_env())
    print(con.execute("CALL start_ui()").fetchone()[0], "(Ctrl+C to stop)", flush=True)
    threading.Event().wait()


if __name__ == "__main__":
    main()
