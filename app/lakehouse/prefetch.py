"""Download first-run artifacts (Spark Kafka jar, DuckDB extensions) so `make stream` starts offline."""

import duckdb

from .stream import KAFKA_PACKAGE, spark_session


def main() -> None:
    duckdb.connect().execute("INSTALL ducklake; INSTALL postgres; INSTALL httpfs")
    spark_session().stop()
    print(f"prefetched DuckDB extensions and {KAFKA_PACKAGE}")


if __name__ == "__main__":
    main()
