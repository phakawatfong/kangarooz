"""Download first-run artifacts (Spark Kafka + Avro jars, DuckDB extensions) so `make stream` starts offline."""

import duckdb

from .stream import SPARK_PACKAGES, spark_session


def main() -> None:
    duckdb.connect().execute("INSTALL ducklake; INSTALL postgres; INSTALL httpfs")
    spark_session("f1-prefetch").stop()
    print(f"prefetched DuckDB extensions and {SPARK_PACKAGES}")


if __name__ == "__main__":
    main()
