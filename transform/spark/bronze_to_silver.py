import hashlib
import hmac
import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from contracts.tables import CONTRACTS, ColumnContract, TableContract
from transform.spark.session import build_session

SPARK_TYPES = {
    "int8": T.LongType(),
    "int4": T.IntegerType(),
    "text": T.StringType(),
    "bpchar": T.StringType(),
    "varchar": T.StringType(),
    "timestamptz": T.TimestampType(),
    "date": T.DateType(),
    "bool": T.BooleanType(),
}


class SilverError(Exception):
    pass


def bronze_root() -> str:
    return os.environ.get("CDC_BRONZE_ROOT", "data/bronze/cdc")


def silver_root() -> str:
    return os.environ.get("SILVER_ROOT", "data/silver")


def masking_key() -> bytes:
    value = os.environ.get("PII_HMAC_KEY", "")
    if not value:
        raise SilverError(
            "PII_HMAC_KEY is not set; silver refuses to run rather than write "
            "personal data in the clear"
        )
    return value.encode("utf-8")


def spark_type(column: ColumnContract):
    if column.type_name == "numeric":
        return T.DecimalType(column.precision or 38, column.scale or 9)
    if column.type_name not in SPARK_TYPES:
        raise SilverError(
            "no Spark type registered for postgres type {!r} on column {}".format(
                column.type_name, column.name
            )
        )
    return SPARK_TYPES[column.type_name]


def assert_castable(frame, contract: TableContract, column: ColumnContract) -> None:
    target = spark_type(column).simpleString()
    source = "raw_{}".format(column.name)
    offenders = (
        frame.select(F.col(source).alias("observed"))
        .where(
            F.col(source).isNotNull()
            & F.expr("try_cast(`{}` as {})".format(source, target)).isNull()
        )
        .limit(1)
        .collect()
    )
    if offenders:
        raise SilverError(
            "column {}.{} carries {!r}, which is not a valid {}; the contract declares "
            "it as postgres {}".format(
                contract.qualified_name,
                column.name,
                offenders[0]["observed"],
                target,
                column.type_name,
            )
        )


def cleaned_history(frame, contract: TableContract):
    keys = [F.col("key").getItem(name).alias("key_{}".format(name)) for name in contract.key_columns]
    projected = frame.select(
        *keys,
        *[
            F.col("after").getItem(column.name).alias("raw_{}".format(column.name))
            for column in contract.columns
        ],
        F.col("action"),
        F.col("lsn"),
        F.col("lsn_numeric"),
        F.col("xid"),
        F.col("commit_time"),
    )
    key_columns = ["key_{}".format(name) for name in contract.key_columns]
    carried = Window.partitionBy(*key_columns).orderBy("lsn_numeric").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    filled = projected
    for column in contract.columns:
        source = "raw_{}".format(column.name)
        filled = filled.withColumn(source, F.last(F.col(source), ignorenulls=True).over(carried))
    latest = Window.partitionBy(*key_columns).orderBy(F.col("lsn_numeric").desc())
    return filled.withColumn("is_current", F.row_number().over(latest) == F.lit(1))


def apply_truncates(rows, truncates):
    if truncates.isEmpty():
        return rows.withColumn("truncated_at_lsn", F.lit(None).cast("string"))
    boundary = truncates.agg(F.max("lsn_numeric").alias("truncate_lsn")).collect()[0]
    marker = truncates.where(F.col("lsn_numeric") == boundary["truncate_lsn"]).select("lsn").first()
    return rows.withColumn(
        "truncated_at_lsn",
        F.when(
            F.col("is_current") & (F.col("lsn_numeric") < F.lit(boundary["truncate_lsn"])),
            F.lit(marker["lsn"]),
        ),
    )


def build_silver(frame, contract: TableContract, mask):
    rows = cleaned_history(frame.where(F.col("action") != "truncate"), contract)
    for column in contract.columns:
        assert_castable(rows, contract, column)
    silver = apply_truncates(rows, frame.where(F.col("action") == "truncate"))
    for column in contract.columns:
        source = F.col("raw_{}".format(column.name))
        value = mask(source) if column.pii else source.cast(spark_type(column))
        silver = silver.withColumn(column.name, value)
    return (
        silver.withColumn(
            "is_deleted",
            (F.col("action") == "delete") | F.col("truncated_at_lsn").isNotNull(),
        )
        .withColumnRenamed("lsn", "change_lsn")
        .withColumnRenamed("lsn_numeric", "change_lsn_numeric")
        .withColumnRenamed("xid", "change_xid")
        .withColumnRenamed("commit_time", "change_commit_time")
        .select(
            *[column.name for column in contract.columns],
            "is_current",
            "is_deleted",
            "truncated_at_lsn",
            "change_lsn",
            "change_lsn_numeric",
            "change_xid",
            "change_commit_time",
        )
    )


def main() -> int:
    key = masking_key()
    spark = build_session("bronze-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    def mask_value(value):
        if value is None:
            return None
        return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()

    mask = F.udf(mask_value, T.StringType())
    written = {}
    try:
        for name, contract in sorted(CONTRACTS.items()):
            source = "{}/{}".format(bronze_root(), contract.name)
            if not os.path.isdir(source):
                print("no bronze for {}, skipping".format(name), file=sys.stderr)
                continue
            silver = build_silver(spark.read.parquet(source), contract, mask)
            target = "{}/{}".format(silver_root(), contract.name)
            silver.write.mode("overwrite").parquet(target)
            written[contract.name] = silver.count()
            print("{}: {} rows".format(name, written[contract.name]), file=sys.stderr)
    finally:
        spark.stop()
    print("silver rows: {}".format(sum(written.values())), file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SilverError as error:
        print("silver error: {}".format(error), file=sys.stderr)
        sys.exit(2)
