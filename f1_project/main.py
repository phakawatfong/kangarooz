"""Convenience entry point: `uv run main.py --rate 500 --duration 60` == `python -m fx_producer.cli`."""

from fx_producer.cli import main

if __name__ == "__main__":
    main()
