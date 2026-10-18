"""Browse the lake in the DuckDB UI (http://localhost:4213) while the stream runs.

    uv run python -m lakehouse.ui
"""

import threading

from logs import setup_logging

from .config import LakeConfig
from .lake import connect


def main() -> None:
    setup_logging()
    db = connect(LakeConfig.from_env())
    print(db.execute("CALL start_ui()").fetchone()[0], "(Ctrl+C to stop)", flush=True)
    threading.Event().wait()


if __name__ == "__main__":
    main()
