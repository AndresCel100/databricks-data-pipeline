"""
Bronze layer — raw data ingestion (Medallion Architecture).

Refactored from the legacy Applebees_Pipeline_bronzelayer notebook into a
Spark Declarative Pipeline Python file.

Ingests the 5 raw Applebee's JSON sources "as-is" from the Unity Catalog
volume /Volumes/workspace/bronze/rawlanding/ using Auto Loader (incremental,
exactly-once), then explodes the nested arrays of the `checks` fact table
into their own bronze tables — preserving the original table names, column
schemas, and audit metadata columns (_ingested_at, _source_file).
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, explode, from_json

# Root of the raw landing zone (one sub-folder per raw source entity).
RAW_LANDING_ROOT = "/Volumes/workspace/bronze/rawlanding"

# ---------------------------------------------------------------------------
# Explicit schemas for the dimension sources whose landing folders are
# currently EMPTY — Auto Loader cannot infer a schema when there are no
# files to sample, so the schema is pinned 1:1 from the legacy bronze tables
# (identical column names and types). When files land later, they are
# ingested incrementally against these schemas.
# ---------------------------------------------------------------------------
_EMPLOYEES_SCHEMA = """
    AlternateEmployeeId STRING,
    Borrowed BOOLEAN,
    DateOfBirth STRING,
    Deleted BOOLEAN,
    HireDate STRING,
    HomeLocationId BIGINT,
    Id BIGINT,
    IsEmancipated STRING,
    IsMinor BOOLEAN,
    JobID ARRAY<BIGINT>,
    LocationEmployeeId STRING,
    LocationId BIGINT,
    Name STRING,
    PayrollEmployeeId STRING,
    PrimaryJobID BIGINT
"""

_ITEMS_TABLE_SCHEMA = """
    ExternalId STRING,
    Id BIGINT,
    Name STRING,
    Plu BIGINT,
    SalesCategoryId BIGINT
"""

_LOCATIONS_SCHEMA = """
    ClosedDate STRING,
    Id BIGINT,
    Name STRING,
    OpenDate STRING,
    TimeZoneName STRING
"""

_REVENUE_CENTERS_SCHEMA = """
    Id BIGINT,
    Name STRING
"""

# Schema of ONE raw checks record (an element of the JSON array inside each
# checks.json file), matching the legacy bronze checks table 1:1 — including
# every nested array (Discounts, ItemsSold, Payments, Tax, Voids). Validated
# against the raw files in rawlanding/checks.
_CHECKS_SCHEMA = """STRUCT<
    BusinessDate STRING,
    CloseTime STRING,
    Discounts ARRAY<STRUCT<Amount DOUBLE, DiscountId BIGINT, DiscountName STRING, EmployeeId BIGINT, ManagerId BIGINT>>,
    Id BIGINT,
    ItemsSold ARRAY<STRUCT<Discounts ARRAY<STRUCT<Amount DOUBLE, DiscountId BIGINT, DiscountName STRING, EmployeeId BIGINT, ManagerId BIGINT>>, EmployeeId BIGINT, GrossPrice DOUBLE, ItemId BIGINT, ItemMajorCatId BIGINT, ItemMajorCatName STRING, ItemName STRING, ItemPLU BIGINT, ItemSubCatId BIGINT, ItemSubCatName STRING, OrderTime STRING, ParentSequence BIGINT, PosSourceId BIGINT, Quantity DOUBLE, RevenueCenterId BIGINT, Seat BIGINT, Sequence BIGINT, SoldPrice DOUBLE>>,
    LocationId BIGINT,
    LoyaltyId STRING,
    OpenTime STRING,
    Payments ARRAY<STRUCT<Amount DOUBLE, EmployeeId BIGINT, Gratuity DOUBLE, IsCashDebit BOOLEAN, IsCreditCard BOOLEAN, IsGiftCard BOOLEAN, IsGiftCert BOOLEAN, IsNonPOS BOOLEAN, IsPettyCash BOOLEAN, PayCatName STRING, PayCatNo BIGINT, PaymentCategoryId BIGINT, PosSourceId BIGINT>>,
    RevenueCenterId BIGINT,
    ServiceCharge BIGINT,
    TableName STRING,
    Tax ARRAY<STRUCT<Amount DOUBLE, TaxCategoryId BIGINT, TaxCategoryName STRING>>,
    TrafficCount DOUBLE,
    Voids ARRAY<STRUCT<Amount DOUBLE, EmployeeId BIGINT, ItemId BIGINT, ItemName STRING, ItemPLU BIGINT, ManagerId BIGINT, Quantity DOUBLE, VoidCategoryId BIGINT, VoidCategoryName STRING>>
>"""


# ============================================================================
# 1. RAW INGESTION — Auto Loader (incremental)
# ============================================================================
# Data is preserved in its original form — no joins, filters, deduplication,
# or transformations are applied. Standard audit metadata columns are
# appended to every table:
#   _ingested_at – timestamp when the row was processed
#   _source_file – path of the source file the row came from
# ============================================================================
def _read_raw(folder: str, schema: str = None) -> DataFrame:
    """
    Incrementally read raw JSON files from RAW_LANDING_ROOT/<folder> as-is.

    - Auto Loader (cloudFiles) discovers new files across runs, including
      nested partition sub-folders (e.g. checks/dt=YYYY-MM-DD/...).
    - multiLine=true: each raw file is a single JSON array of records.
    - inferColumnTypes=true: infer real Spark types instead of all STRING
      (only used when the schema is inferred; folder has data).
    - schema: optional explicit schema, required for sources whose landing
      folder is currently empty (no files to infer from).
    - _rescued_data (auto-added by schema inference) is dropped to keep the
      exact legacy schema.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
    )
    if schema is not None:
        reader = reader.schema(schema)
    return (
        reader.load(f"{RAW_LANDING_ROOT}/{folder}")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .drop("_rescued_data")
    )


# ---------------------------------------------------------------------------
# `checks` uses the TEXT format + from_json instead of the JSON reader:
# each raw file (71-168 MB) is a single JSON array, and the JSON reader's
# whole-file (multiLine) parse exceeds the per-task memory budget of
# serverless compute. Reading each file as one text line and parsing with
# from_json (JVM parser) has a much smaller memory footprint, while Auto
# Loader still provides incremental, exactly-once file discovery across
# the dt=* partition sub-folders.
# ---------------------------------------------------------------------------
@dp.table(
    name="checks",
    comment=(
        "Raw checks fact data loaded as-is from rawlanding/checks; Auto Loader "
        "discovers files recursively across the dt=* partition sub-folders."
    ),
    # Each file is a single ~71-168 MB line: one file per scan task.
    spark_conf={"spark.sql.files.maxPartitionBytes": "16MB"},
)
def checks():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.maxFilesPerTrigger", 5)
        # Pin the text reader schema explicitly: Auto Loader would otherwise
        # reuse the schema persisted by the earlier JSON-format runs of this
        # flow (checks business columns) instead of the fixed text schema.
        # dt is declared here because cloudFiles.partitionColumns requires
        # the partition column to exist in the provided schema; DATE matches
        # the partition value format (dt=YYYY-MM-DD) and the existing table.
        .schema("value STRING, dt DATE")
        # dt=YYYY-MM-DD partition folders under rawlanding/checks.
        .option("cloudFiles.partitionColumns", "dt")
        .load(f"{RAW_LANDING_ROOT}/checks")
    )
    return (
        raw.select(
            from_json(col("value"), f"ARRAY<{_CHECKS_SCHEMA}>").alias("records"),
            col("dt"),
            col("_metadata.file_path").alias("_source_file"),
        )
        .select(explode(col("records")).alias("check"), col("dt"), col("_source_file"))
        .select(
            "check.*",
            "dt",
            current_timestamp().alias("_ingested_at"),
            "_source_file",
        )
    )


@dp.table(
    name="employees",
    comment="Raw employees dimension loaded as-is from rawlanding/employees.",
)
def employees():
    return _read_raw("employees", _EMPLOYEES_SCHEMA)


@dp.table(
    name="items_table",
    comment="Raw items dimension loaded as-is from rawlanding/items.",
)
def items_table():
    return _read_raw("items", _ITEMS_TABLE_SCHEMA)


@dp.table(
    name="locations_with_coordinates",
    comment=(
        "Raw locations dimension loaded as-is from rawlanding/locations."
    ),
)
def locations_with_coordinates():
    return _read_raw("locations", _LOCATIONS_SCHEMA)


@dp.table(
    name="revenue_centers",
    comment=(
        "Raw revenue centers dimension loaded as-is from rawlanding/revenue_centers."
    ),
)
def revenue_centers():
    return _read_raw("revenue_centers", _REVENUE_CENTERS_SCHEMA)


# ============================================================================
# 2. EXPLODE NESTED ARRAY COLUMNS FROM `checks`
# ============================================================================
# The raw `checks` fact table stores line-level detail as nested arrays.
# Each array column is exploded into its own bronze table:
#   checks_discounts ← Discounts
#   checks_items     ← ItemsSold
#   checks_payments  ← Payments
#   checks_tax       ← Tax
#   checks_voids      ← Voids
# Struct array elements are flattened into top-level columns, keeping the
# exact same column names and order as the legacy notebook.
# ============================================================================
def _explode_checks_array(array_col: str) -> DataFrame:
    """
    Read `checks` incrementally, keep the _source_file lineage column,
    explode the given nested array column, flatten the struct element
    fields into top-level columns, and stamp _ingested_at.
    """
    return (
        spark.readStream.table("checks")
        .select(col("_source_file"), explode(col(array_col)).alias(array_col))
        .select("_source_file", f"{array_col}.*")
        .withColumn("_ingested_at", current_timestamp())
    )


@dp.table(
    name="checks_discounts",
    comment="Discounts array exploded out of the raw checks fact table.",
)
def checks_discounts():
    return _explode_checks_array("Discounts")


@dp.table(
    name="checks_items",
    comment="ItemsSold array exploded out of the raw checks fact table.",
)
def checks_items():
    return _explode_checks_array("ItemsSold")


@dp.table(
    name="checks_payments",
    comment="Payments array exploded out of the raw checks fact table.",
)
def checks_payments():
    return _explode_checks_array("Payments")


@dp.table(
    name="checks_tax",
    comment="Tax array exploded out of the raw checks fact table.",
)
def checks_tax():
    return _explode_checks_array("Tax")


@dp.table(
    name="checks_voids",
    comment="Voids array exploded out of the raw checks fact table.",
)
def checks_voids():
    return _explode_checks_array("Voids")
