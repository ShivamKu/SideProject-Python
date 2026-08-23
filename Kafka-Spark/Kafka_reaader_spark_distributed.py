#!/usr/bin/env python3
"""
kafka_reader_batch_distributed.py

Distributed PySpark batch reader for large Kafka extracts.

Key behavior
------------
* BATCH ONLY.
* No --from-time:
      read from earliest retained Kafka offsets.
* No --to-time:
      read through latest offsets captured when the batch starts.
* --from-time / --to-time:
      translated to Kafka startingTimestamp / endingTimestamp so Spark does
      NOT need to scan records outside the requested Kafka timestamp window.
* Filtering is done with native Spark SQL functions - no Python UDF.
* Kafka value may contain framing bytes/text around an embedded JSON object.
  The JSON object is extracted from first "{" through last "}".
* Output is written distributed to S3/HDFS/local filesystem.
* No collect(), toPandas(), global orderBy(), or mandatory count().

Examples
--------

Entire Kafka retention:
spark-submit ... kafka_reader_batch_distributed.py \
  --connection kafka_connection_v3.properties \
  --topic revenue-management.yield-management.group-block.merge \
  --group-id revenue-management.yield-management.oyty-group-block-merge.consumer-group \
  --filter propertyCode=DALBR \
  --filter groupCode=BHA \
  --min-partitions 120 \
  --output s3://bucket/kafka_extract/DALBR/ \
  --output-partitions 24 \
  --save-payload-only

Time range:
spark-submit ... kafka_reader_batch_distributed.py \
  --connection kafka_connection_v3.properties \
  --topic revenue-management.yield-management.group-block.merge \
  --group-id revenue-management.yield-management.oyty-group-block-merge.consumer-group \
  --from-time "2026-08-12 00:00:00" \
  --to-time "2026-08-13 00:00:00" \
  --filter propertyCode=DALBR \
  --output s3://bucket/kafka_extract/DALBR/20260812/ \
  --save-payload-only
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Distributed PySpark batch Kafka JSON extractor"
    )

    parser.add_argument(
        "--connection",
        required=True,
        help="Flat Kafka .properties connection file"
    )

    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic to read"
    )

    parser.add_argument(
        "--group-id",
        help=(
            "Authorized Kafka consumer group. Overrides group.id from "
            "the connection properties file."
        )
    )

    parser.add_argument(
        "--from-time",
        help=(
            "Optional inclusive Kafka timestamp lower bound. "
            "Examples: '2026-08-12 00:00:00', "
            "'2026-08-12T00:00:00Z', or epoch milliseconds."
        )
    )

    parser.add_argument(
        "--to-time",
        help=(
            "Optional exclusive Kafka timestamp upper bound. "
            "Examples: '2026-08-13 00:00:00', "
            "'2026-08-13T00:00:00Z', or epoch milliseconds."
        )
    )

    parser.add_argument(
        "--time-zone",
        default="UTC",
        help=(
            "Timezone used for --from-time/--to-time when the supplied "
            "datetime has no timezone. Default: UTC"
        )
    )

    parser.add_argument(
        "--filter",
        action="append",
        default=[],
        help=(
            "JSON field filter. Repeat as needed. "
            "Examples: --filter propertyCode=DALBR "
            "--filter groupCode=BHA "
            "--filter 'actionIndicator=C|U' "
            "--filter 'groupContracts[].code=BHA'"
        )
    )

    parser.add_argument(
        "--min-partitions",
        type=int,
        default=0,
        help=(
            "Desired minimum Spark Kafka read partitions. "
            "0 means use Kafka topic partitioning. "
            "For large scans, values such as 60/120 can increase parallelism."
        )
    )

    parser.add_argument(
        "--output",
        help="Output path, e.g. s3://bucket/kafka_extract/DALBR/"
    )

    parser.add_argument(
        "--output-format",
        choices=["json", "parquet"],
        default="json",
        help="Enriched output format. Default: json"
    )

    parser.add_argument(
        "--output-mode",
        choices=["overwrite", "append"],
        default="overwrite"
    )

    parser.add_argument(
        "--output-partitions",
        type=int,
        default=0,
        help=(
            "Optional number of output partitions/files. "
            "0 preserves current partitioning. Use e.g. 24/48 for large output."
        )
    )

    parser.add_argument(
        "--save-payload-only",
        action="store_true",
        help="Write only clean extracted JSON messages as text/JSONL part files"
    )

    parser.add_argument(
        "--show-raw",
        type=int,
        default=0,
        help="Debug only: display N raw Kafka values"
    )

    parser.add_argument(
        "--show-matched",
        type=int,
        default=0,
        help="Debug only: display N matched rows"
    )

    parser.add_argument(
        "--count-results",
        action="store_true",
        help=(
            "Count matched rows before writing. Causes an additional full "
            "Spark action; leave disabled for large production extracts."
        )
    )

    parser.add_argument(
        "--fail-on-data-loss",
        choices=["true", "false"],
        default="false"
    )

    parser.add_argument(
        "--starting-offsets-by-timestamp-strategy",
        choices=["error", "latest"],
        default="latest",
        help=(
            "Behavior when a requested from-time has no matching retained "
            "offset for a partition. Default: latest"
        )
    )

    parser.add_argument(
        "--poll-timeout-ms",
        type=int,
        default=120000
    )

    parser.add_argument(
        "--session-timeout-ms",
        type=int,
        default=10000,
        help=(
            "Kafka consumer session timeout. A relatively small value helps "
            "reduce interference when an explicit kafka.group.id is required."
        )
    )

    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print non-sensitive resolved configuration"
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

def load_properties(path: str) -> Dict[str, str]:
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"Connection file not found: {path}")

    props: Dict[str, str] = {}

    for line_number, original in enumerate(
        file_path.read_text(encoding="utf-8").splitlines(),
        start=1
    ):
        line = original.strip()

        if not line or line.startswith("#") or line.startswith(";"):
            continue

        if "=" not in line:
            raise ValueError(
                f"Invalid properties line {line_number}: expected key=value"
            )

        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()

    return props


def first_present(props: Dict[str, str], *keys: str) -> Optional[str]:
    for key in keys:
        value = props.get(key)
        if value is not None and value != "":
            return value
    return None


def require(props: Dict[str, str], *keys: str) -> str:
    value = first_present(props, *keys)

    if value is None:
        raise ValueError(
            "Missing required Kafka property. Expected one of: "
            + ", ".join(keys)
        )

    return value


# ---------------------------------------------------------------------------
# Kafka OAuth
# ---------------------------------------------------------------------------

def jaas_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_jaas_config(props: Dict[str, str]) -> str:
    client_id = require(
        props,
        "client.id",
        "sasl.oauthbearer.client.id"
    )

    client_secret = require(
        props,
        "client.secret",
        "sasl.oauthbearer.client.secret"
    )

    scope = first_present(
        props,
        "scope",
        "sasl.oauthbearer.scope"
    )

    logical_cluster = first_present(
        props,
        "logical.cluster",
        "logicalCluster"
    )

    identity_pool = first_present(
        props,
        "identity.pool.id",
        "identityPoolId"
    )

    parts = [
        "org.apache.kafka.common.security.oauthbearer."
        "OAuthBearerLoginModule required",
        f'clientId="{jaas_escape(client_id)}"',
        f'clientSecret="{jaas_escape(client_secret)}"',
    ]

    if scope:
        parts.append(f'scope="{jaas_escape(scope)}"')

    if logical_cluster:
        parts.append(
            f'extension_logicalCluster="{jaas_escape(logical_cluster)}"'
        )

    if identity_pool:
        parts.append(
            f'extension_identityPoolId="{jaas_escape(identity_pool)}"'
        )

    return " ".join(parts) + ";"


def build_kafka_options(
    props: Dict[str, str],
    args
) -> Dict[str, str]:

    bootstrap = require(props, "bootstrap.servers")

    security_protocol = (
        first_present(props, "security.protocol") or "SASL_SSL"
    )

    mechanism = (
        first_present(props, "sasl.mechanism") or "OAUTHBEARER"
    )

    group_id = (
        args.group_id
        or first_present(props, "group.id", "consumer.group.id")
    )

    if not group_id:
        raise ValueError(
            "No Kafka group id supplied. Use --group-id or group.id "
            "in the connection properties file."
        )

    options: Dict[str, str] = {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": args.topic,
        "kafka.security.protocol": security_protocol,
        "kafka.sasl.mechanism": mechanism,
        "kafka.group.id": group_id,
        "kafka.session.timeout.ms": str(args.session_timeout_ms),
        "kafkaConsumer.pollTimeoutMs": str(args.poll_timeout_ms),
        "failOnDataLoss": args.fail_on_data_loss,
    }

    if args.min_partitions and args.min_partitions > 0:
        options["minPartitions"] = str(args.min_partitions)

    if mechanism.upper() == "OAUTHBEARER":
        token_endpoint = require(
            props,
            "sasl.oauthbearer.token.endpoint.url"
        )

        callback_class = require(
            props,
            "sasl.login.callback.handler.class"
        )

        options[
            "kafka.sasl.oauthbearer.token.endpoint.url"
        ] = token_endpoint

        options[
            "kafka.sasl.login.callback.handler.class"
        ] = callback_class

        explicit_jaas = first_present(
            props,
            "sasl.jaas.config"
        )

        options["kafka.sasl.jaas.config"] = (
            explicit_jaas
            if explicit_jaas
            else build_jaas_config(props)
        )

    passthrough = [
        "ssl.truststore.location",
        "ssl.truststore.password",
        "ssl.truststore.type",
        "ssl.keystore.location",
        "ssl.keystore.password",
        "ssl.keystore.type",
        "ssl.key.password",
        "ssl.endpoint.identification.algorithm",
        "sasl.login.connect.timeout.ms",
        "sasl.login.read.timeout.ms",
        "sasl.login.retry.backoff.ms",
        "sasl.login.retry.backoff.max.ms",
    ]

    for key in passthrough:
        if key in props:
            options[f"kafka.{key}"] = props[key]

    return options


# ---------------------------------------------------------------------------
# Timestamp handling
# ---------------------------------------------------------------------------

def timestamp_to_epoch_millis(value: str, default_tz: str) -> str:
    """
    Accept:
      1723420800000
      2026-08-12 00:00:00
      2026-08-12T00:00:00
      2026-08-12T00:00:00Z
      2026-08-12T00:00:00+00:00
    """

    stripped = value.strip()

    if stripped.isdigit():
        return stripped

    normalized = stripped

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"Invalid timestamp '{value}'. Use ISO datetime such as "
            "'2026-08-12 00:00:00' or epoch milliseconds."
        ) from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(default_tz))

    epoch_ms = int(dt.timestamp() * 1000)
    return str(epoch_ms)


def apply_batch_range_options(
    options: Dict[str, str],
    args
) -> Dict[str, str]:
    """
    Use Kafka offset-by-timestamp selection when time arguments are supplied.

    No from-time => earliest retained offset.
    No to-time   => latest offset at batch start.
    """

    resolved = dict(options)

    if args.from_time:
        resolved["startingTimestamp"] = timestamp_to_epoch_millis(
            args.from_time,
            args.time_zone
        )
        resolved[
            "startingOffsetsByTimestampStrategy"
        ] = args.starting_offsets_by_timestamp_strategy
    else:
        resolved["startingOffsets"] = "earliest"

    if args.to_time:
        resolved["endingTimestamp"] = timestamp_to_epoch_millis(
            args.to_time,
            args.time_zone
        )
    else:
        resolved["endingOffsets"] = "latest"

    return resolved


def validate_time_range(args):
    if args.from_time and args.to_time:
        start = int(timestamp_to_epoch_millis(args.from_time, args.time_zone))
        end = int(timestamp_to_epoch_millis(args.to_time, args.time_zone))

        if end <= start:
            raise ValueError("--to-time must be later than --from-time")


# ---------------------------------------------------------------------------
# Kafka DataFrame
# ---------------------------------------------------------------------------

def create_kafka_batch_df(
    spark: SparkSession,
    options: Dict[str, str]
) -> DataFrame:

    reader = spark.read.format("kafka")

    for key, value in options.items():
        reader = reader.option(key, value)

    return reader.load()


# ---------------------------------------------------------------------------
# Embedded JSON
# ---------------------------------------------------------------------------

def with_embedded_json(kafka_df: DataFrame) -> DataFrame:
    """
    Observed message form:
       <framing>{"propertyCode":"SATSA",...}<suffix>

    Extract the JSON object using native Spark regexp_extract.
    """

    return (
        kafka_df
        .withColumn(
            "_raw_value",
            F.col("value").cast("string")
        )
        .withColumn(
            "_json_body",
            F.regexp_extract(
                F.col("_raw_value"),
                r"(?s)(\{.*\})",
                1
            )
        )
        .filter(
            F.length(F.col("_json_body")) > 0
        )
    )


def show_raw_samples(kafka_df: DataFrame, count: int):
    if count <= 0:
        return

    (
        kafka_df
        .select(
            "partition",
            "offset",
            "timestamp",
            F.col("value").cast("string").alias("_raw_value")
        )
        .show(count, truncate=False)
    )


# ---------------------------------------------------------------------------
# JSON filters
# ---------------------------------------------------------------------------

def parse_filter_expression(
    expression: str
) -> Tuple[str, List[str]]:

    if "=" not in expression:
        raise ValueError(
            f"Invalid --filter '{expression}'. Expected field=value"
        )

    field_name, raw_values = expression.split("=", 1)
    field_name = field_name.strip()

    if not field_name:
        raise ValueError(
            f"Invalid --filter '{expression}': empty field"
        )

    values = [
        value.strip()
        for value in raw_values.split("|")
    ]

    return field_name, values


def to_json_path(field_name: str) -> str:
    path = field_name.replace("[]", "[*]")

    if not path.startswith("$"):
        path = "$." + path

    return path


def apply_json_filters(
    df: DataFrame,
    expressions: List[str]
) -> DataFrame:

    result = df

    for expression in expressions:
        field_name, values = parse_filter_expression(expression)

        extracted = F.get_json_object(
            F.col("_json_body"),
            to_json_path(field_name)
        )

        if "[]" in field_name:
            array_value = F.from_json(
                extracted,
                "array<string>"
            )

            condition = None

            for value in values:
                one_condition = F.array_contains(
                    array_value,
                    value
                )

                condition = (
                    one_condition
                    if condition is None
                    else condition | one_condition
                )

            # Some Spark/Kafka JSON shapes can return a scalar for a single
            # wildcard result. Keep a scalar fallback.
            result = result.filter(
                condition | extracted.isin(values)
            )

        else:
            result = result.filter(
                extracted.isin(values)
            )

    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def prepare_output(
    df: DataFrame,
    output_partitions: int
) -> DataFrame:

    if output_partitions and output_partitions > 0:
        return df.repartition(output_partitions)

    return df


def write_output(df: DataFrame, args):
    if not args.output:
        return

    output_df = prepare_output(
        df,
        args.output_partitions
    )

    if args.save_payload_only:
        (
            output_df
            .select(
                F.col("_json_body").alias("value")
            )
            .write
            .mode(args.output_mode)
            .text(args.output)
        )

        return

    enriched = output_df.select(
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.col("key").cast("string").alias("_kafka_key"),
        F.col("_json_body")
    )

    writer = enriched.write.mode(
        args.output_mode
    )

    if args.output_format == "parquet":
        writer.parquet(args.output)
    else:
        writer.json(args.output)


# ---------------------------------------------------------------------------
# Safe config
# ---------------------------------------------------------------------------

def print_safe_config(
    args,
    options: Dict[str, str]
):
    print("\n=== Kafka batch configuration ===")
    print(f"Topic                : {args.topic}")
    print(f"Group ID             : {options.get('kafka.group.id')}")
    print(f"Bootstrap            : {options.get('kafka.bootstrap.servers')}")
    print(f"Security protocol    : {options.get('kafka.security.protocol')}")
    print(f"SASL mechanism       : {options.get('kafka.sasl.mechanism')}")
    print(f"From time            : {args.from_time or 'EARLIEST RETAINED OFFSET'}")
    print(f"To time              : {args.to_time or 'LATEST AT BATCH START'}")
    print(f"Timezone             : {args.time_zone}")
    print(f"minPartitions        : {args.min_partitions or 'Kafka partition count'}")
    print(f"Output partitions    : {args.output_partitions or 'preserve current'}")

    if args.from_time:
        print(
            f"startingTimestamp(ms): "
            f"{options.get('startingTimestamp')}"
        )

    if args.to_time:
        print(
            f"endingTimestamp(ms)  : "
            f"{options.get('endingTimestamp')}"
        )

    if args.filter:
        print("Filters:")
        for expression in args.filter:
            print(f"  - {expression}")

    print("Client secret        : ***")
    print("JAAS config          : ***")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    validate_time_range(args)

    props = load_properties(
        args.connection
    )

    kafka_options = build_kafka_options(
        props,
        args
    )

    kafka_options = apply_batch_range_options(
        kafka_options,
        args
    )

    if args.print_config:
        print_safe_config(
            args,
            kafka_options
        )

    spark = (
        SparkSession.builder
        .appName("DistributedKafkaBatchExtractor")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\nCreating distributed Kafka batch DataFrame...")

        kafka_df = create_kafka_batch_df(
            spark,
            kafka_options
        )

        if args.show_raw > 0:
            print(
                f"\n=== First {args.show_raw} Kafka values "
                "(debug action) ==="
            )
            show_raw_samples(
                kafka_df,
                args.show_raw
            )

        json_df = with_embedded_json(
            kafka_df
        )

        matched_df = apply_json_filters(
            json_df,
            args.filter
        )

        if args.count_results:
            # Explicitly optional because it causes a full Spark action.
            matched_count = matched_df.count()
            print(f"\nMatched rows: {matched_count}")

        if args.show_matched > 0:
            print(
                f"\n=== First {args.show_matched} matched rows "
                "(debug action) ==="
            )
            (
                matched_df
                .select(
                    "partition",
                    "offset",
                    "timestamp",
                    "_json_body"
                )
                .show(
                    args.show_matched,
                    truncate=False
                )
            )

        if args.output:
            print(
                f"\nWriting matched rows to: {args.output}"
            )

            write_output(
                matched_df,
                args
            )

            print("Batch write completed successfully.")
        elif not args.count_results and args.show_matched <= 0:
            print(
                "\nNo output/count/show action requested. "
                "Spark DataFrames are lazy, so no full Kafka scan was executed."
            )

    except Exception as exc:
        print(
            f"\nERROR while processing Kafka batch: {exc}",
            file=sys.stderr
        )
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
