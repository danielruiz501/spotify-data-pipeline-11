import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# --------------------------------------------------
# 1. Initialize Glue and Spark
# --------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# 2. S3 paths
# --------------------------------------------------
bucket = "aws-data-engineer-daniel"

msk_path = f"s3://{bucket}/raw-msk/"
kinesis_path = f"s3://{bucket}/raw-kinesis/"
output_path = f"s3://{bucket}/processed-tracks/"

# --------------------------------------------------
# 3. Common schema for empty DataFrames
# --------------------------------------------------
output_schema = StructType.fromJson({
    "type": "struct",
    "fields": [
        {"name": "event_id", "type": "string", "nullable": True, "metadata": {}},
        {"name": "event_timestamp", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "track_name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "artist_name", "type": "string", "nullable": True, "metadata": {}},
        {"name": "genre", "type": "string", "nullable": True, "metadata": {}},
        {"name": "popularity", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "duration_ms", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "source", "type": "string", "nullable": True, "metadata": {}}
    ]
})


# --------------------------------------------------
# 4. Normalize song events
# --------------------------------------------------
def normalize_tracks(df, source):
    available = set(df.columns)

    def string_field(name):
        if name in available:
            return F.col(name).cast("string").alias(name)
        return F.lit(None).cast("string").alias(name)

    def integer_field(name):
        if name in available:
            return F.col(name).cast("int").alias(name)
        return F.lit(None).cast("int").alias(name)

    # Parse ISO-8601 timestamps, including UTC offsets.
    if "event_timestamp" in available:
        timestamp_field = F.to_timestamp(
            F.col("event_timestamp").cast("string")
        ).alias("event_timestamp")
    else:
        timestamp_field = F.lit(None).cast("timestamp").alias(
            "event_timestamp"
        )

    return df.select(
        string_field("event_id"),
        timestamp_field,
        string_field("track_name"),
        string_field("artist_name"),
        string_field("genre"),
        integer_field("popularity"),
        integer_field("duration_ms"),
        F.lit(source).cast("string").alias("source")
    )


# --------------------------------------------------
# 5. Read MSK JSON events
# --------------------------------------------------
msk_raw = (
    spark.read
    .option("multiLine", "true")
    .json(msk_path)
)

print("MSK input schema:")
msk_raw.printSchema()
print("MSK input records:", msk_raw.count())

msk_tracks = normalize_tracks(msk_raw, "MSK")


# --------------------------------------------------
# 6. Read Kinesis JSON exports
# --------------------------------------------------
kinesis_raw = (
    spark.read
    .option("multiLine", "true")
    .json(kinesis_path)
)

print("Kinesis input schema:")
kinesis_raw.printSchema()
print("Kinesis input records:", kinesis_raw.count())

# Kinesis files contain a wrapper with an "event" object.
if (
    "event" in kinesis_raw.columns
    and isinstance(kinesis_raw.schema["event"].dataType, StructType)
):
    event_fields = set(
        kinesis_raw.schema["event"].dataType.fieldNames()
    )

    if "track_name" in event_fields:
        kinesis_events = kinesis_raw.select("event.*")
        kinesis_tracks = normalize_tracks(kinesis_events, "Kinesis")
    else:
        print("Skipping Kinesis: nested event has no track_name.")
        kinesis_tracks = spark.createDataFrame([], output_schema)
else:
    print("Skipping Kinesis: no nested event structure found.")
    kinesis_tracks = spark.createDataFrame([], output_schema)


# --------------------------------------------------
# 7. Combine and clean events
# --------------------------------------------------
tracks = msk_tracks.unionByName(kinesis_tracks)

# Discard test records and incomplete song events.
tracks = tracks.filter(
    F.col("track_name").isNotNull()
    & F.col("artist_name").isNotNull()
    & F.col("event_id").isNotNull()
)

# Remove duplicate event IDs.
tracks = tracks.dropDuplicates(["event_id"])

# Add processing timestamp.
tracks = tracks.withColumn(
    "processed_at",
    F.current_timestamp()
)

print("Combined valid tracks:", tracks.count())

print("Tracks by source:")
tracks.groupBy("source").count().show()

tracks.show(50, truncate=False)


# --------------------------------------------------
# 8. Validate final schema
# --------------------------------------------------
print("Final output schema:")
tracks.printSchema()

expected_columns = [
    "event_id",
    "event_timestamp",
    "track_name",
    "artist_name",
    "genre",
    "popularity",
    "duration_ms",
    "source",
    "processed_at"
]

actual_columns = tracks.columns

if actual_columns != expected_columns:
    raise ValueError(
        f"Unexpected output columns: {actual_columns}"
    )

# Fail early if timestamps or numeric fields failed conversion.
tracks.select(
    "event_timestamp",
    "popularity",
    "duration_ms"
).show(50, truncate=False)


# --------------------------------------------------
# 9. Write Parquet output to S3
# --------------------------------------------------
(
    tracks.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(output_path)
)

print("Parquet output:", output_path)

job.commit()
