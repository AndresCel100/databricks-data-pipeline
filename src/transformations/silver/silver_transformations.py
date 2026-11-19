# ============================================================================
# Silver Layer — Cleaning, Deduplication & Fraud Detection
# ----------------------------------------------------------------------------
# Lakeflow Spark Declarative Pipeline refactor of the legacy
# `Applebees_Pipeline_silverlayer` notebook.
#
# Reads the raw Bronze tables from `workspace.bronze` and publishes cleaned,
# typed, and fraud-flagged tables to `workspace.silver`.
#
# Dataset types
#   * Materialized views (batch `spark.read.table`) — needed wherever the
#     legacy logic requires full-history operations a stream cannot do:
#       - `checks` — deduplication on the composite key plus the top-5 %
#         discount-employee percentile aggregation (hypothesis f)
#       - `employee_discount_summary` — explode + GROUP BY aggregation
#       - `suspect_fraud_checks` — filter over the `checks` materialized
#         view (an MV cannot be read with `readStream`)
#   * Streaming tables (incremental `spark.readStream.table`) — row-wise
#     cleaning only: `quarantine_bad_data` plus the 9 pass-through tables
#   * `silver_checks_base` — pipeline-private temporary view holding the
#     shared checks chain: dedup -> type casts -> trim -> 12 derived
#     columns -> fraud flags (a-e) -> column cleanup.
#
# Data quality (legacy .filter() checks -> declarative expectations)
#   * `valid_primary_keys` on `workspace.silver.checks` (expect_or_drop) —
#     replaces the legacy NULL-PK filter (cell 1b) and the
#     checks_pk_not_null Delta constraint; offending rows are also captured
#     in `workspace.silver.quarantine_bad_data` by that table's own filter.
#   * `is_quarantine_record` on `quarantine_bad_data` (expect, non-blocking)
#     — warns in metrics if a row without a NULL primary key ever reaches
#     the quarantine table.
#
# Data-dependent cleaning
#   The legacy helpers `drop_all_null_columns` / `fill_numeric_nulls`
#   profiled the data at runtime with `.count()`; dataset functions in a
#   declarative pipeline cannot run actions, so the profiling result for
#   the current bronze data is encoded below (profiled 2026-11-18):
#     * `checks`: `LoyaltyId` is 100 % NULL -> dropped (the legacy silver
#       table dropped it too); no numeric column has a 0-5 % NULL rate,
#       so nothing is filled with 0.
#     * The 5 exploded child tables contain no NULL values at all.
#     * The 4 dimension tables are empty (0 rows) — the legacy helpers are
#       no-ops on empty inputs (they return the DataFrame unchanged), so
#       every column is kept (including `IsEmancipated` / `ExternalId`,
#       which the legacy run dropped as all-null once data existed).
#   Re-profile and update ALL_NULL_COLUMNS / NUMERIC_ZERO_FILL_COLUMNS when
#   new data lands in bronze.
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, when, hour, trim, expr, explode,
    sum as _sum, count as _count, percentile_approx, collect_set,
)
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Configuration (from the legacy notebook)
# ---------------------------------------------------------------------------
BRONZE_SCHEMA = "workspace.bronze"
SILVER_SCHEMA = "workspace.silver"

# Per-table type-casting rules: {table_name: {column_name: "target_sql_type"}}
# These are applied in addition to the common string-trim and all-null-column drops.
TYPE_CAST_RULES = {
    "checks": {
        "BusinessDate":  "DATE",
        "OpenTime":      "TIMESTAMP",
        "CloseTime":     "TIMESTAMP",
        "ServiceCharge": "DOUBLE",        # monetary field currently stored as LONG
    },
    "employees": {
        "DateOfBirth": "DATE",
        "HireDate":    "DATE",
    },
    "locations_with_coordinates": {
        "OpenDate":   "DATE",
        "ClosedDate": "DATE",
    },
    "checks_items": {
        "OrderTime": "TIMESTAMP",
    },
}

# Fraud flag columns in table column order (hypotheses a-f).
FRAUD_FLAG_COLS = [
    "is_fraud_high_value_excess_discount",
    "is_fraud_low_value_excess_discount",
    "is_fraud_suspicious_in_store",
    "is_fraud_negative_values",
    "is_fraud_irregular_hours",
    "is_fraud_top_discount_employee",
]

# Columns that are 100 % NULL in the current bronze data (profiled 2026-11-18)
# — the static equivalent of the legacy drop_all_null_columns() helper.
ALL_NULL_COLUMNS = {
    "checks": ["LoyaltyId"],
}

# Scalar numeric columns whose NULL rate is between 0 % and 5 % (profiled
# 2026-11-18), filled with 0 — the static equivalent of fill_numeric_nulls().
# No column currently qualifies.
NUMERIC_ZERO_FILL_COLUMNS = {}

# ---------------------------------------------------------------------------
# Helpers (pure DataFrame transformations — no actions)
# ---------------------------------------------------------------------------
def apply_type_casts(df, table_name):
    """Cast columns to the target types specified in TYPE_CAST_RULES."""
    rules = TYPE_CAST_RULES.get(table_name, {})
    for col_name, target_type in rules.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(target_type))
    return df


def trim_string_columns(df):
    """Apply trim() to all top-level StringType columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


def drop_all_null_columns(df, table_name):
    """Drop the columns that are 100 % NULL (static list — see above)."""
    null_cols = ALL_NULL_COLUMNS.get(table_name, [])
    return df.drop(*null_cols) if null_cols else df


def fill_numeric_nulls(df, table_name):
    """Fill scalar-numeric NULLs (< 5 % missing) with 0 (static list)."""
    fill_map = NUMERIC_ZERO_FILL_COLUMNS.get(table_name, {})
    return df.fillna(fill_map) if fill_map else df


def clean_bronze_table(df, table_name):
    """Common cleaning chain: type casts -> trim -> drop all-null -> fill."""
    df = apply_type_casts(df, table_name)
    df = trim_string_columns(df)
    df = drop_all_null_columns(df, table_name)
    df = fill_numeric_nulls(df, table_name)
    return df


# ---------------------------------------------------------------------------
# checks — shared transformation chain (pipeline-private temporary view)
# ---------------------------------------------------------------------------
@dp.temporary_view(
    name="silver_checks_base",
    comment=(
        "Deduplicated, typed checks with 12 derived aggregate columns and fraud "
        "flags (a-e); shared base of workspace.silver.checks."
    ),
)
def silver_checks_base():
    checks_bronze = spark.read.table(f"{BRONZE_SCHEMA}.checks")

    # Id (Check ID) is local to each restaurant location and date, so the
    # triple (LocationId, BusinessDate, Id) uniquely identifies a check.
    # Records with NULL primary keys are dropped from
    # workspace.silver.checks by the `valid_primary_keys` expectation and
    # captured in workspace.silver.quarantine_bad_data.
    df = checks_bronze.dropDuplicates(["LocationId", "BusinessDate", "Id"])

    # Type casting (dates, timestamps, monetary) + string trim
    df = apply_type_casts(df, "checks")
    df = trim_string_columns(df)

    # Derived aggregate columns from the nested arrays (legacy section 1f)
    df = (df
        .withColumn("total_discount_amount",
            expr("aggregate(transform(coalesce(Discounts, array()), x -> coalesce(x.Amount, 0.0)), 0.0D, (acc, x) -> acc + x)"))
        .withColumn("total_payment_amount",
            expr("aggregate(transform(coalesce(Payments, array()), x -> coalesce(x.Amount, 0.0)), 0.0D, (acc, x) -> acc + x)"))
        .withColumn("total_gratuity",
            expr("aggregate(transform(coalesce(Payments, array()), x -> coalesce(x.Gratuity, 0.0)), 0.0D, (acc, x) -> acc + x)"))
        .withColumn("total_void_amount",
            expr("aggregate(transform(coalesce(Voids, array()), x -> coalesce(x.Amount, 0.0)), 0.0D, (acc, x) -> acc + x)"))
        .withColumn("item_count", expr("size(coalesce(ItemsSold, array()))"))
        .withColumn("has_voids", expr("size(coalesce(Voids, array())) > 0"))
        .withColumn("has_discounts", expr("size(coalesce(Discounts, array())) > 0"))
        # Check duration in minutes (CloseTime − OpenTime)
        .withColumn("check_duration_minutes",
            (col("CloseTime").cast("long") - col("OpenTime").cast("long")) / 60.0)
        # Hour-of-day the check was opened (for irregular-hours detection)
        .withColumn("open_hour", hour(col("OpenTime")))
        # Any item has a SIGNIFICANTLY negative SoldPrice (< −$20) — half the
        # ~$40 average check, unlikely to be routine comps/voids.
        .withColumn("has_negative_item_price",
            expr("exists(coalesce(ItemsSold, array()), x -> x.SoldPrice < -20.0)"))
        # Any payment has a SIGNIFICANTLY negative Gratuity (< −$20) — far
        # beyond routine POS tip adjustments.
        .withColumn("has_negative_gratuity",
            expr("exists(coalesce(Payments, array()), x -> x.Gratuity < -20.0)"))
    )

    # Discount-to-payment ratio (guard against division by zero)
    df = df.withColumn(
        "discount_ratio",
        when(col("total_payment_amount") > 0,
             col("total_discount_amount") / col("total_payment_amount")
        ).otherwise(lit(0.0)),
    )

    # Fraud flags a-e (legacy section 1g):
    #   (a) payment > $100 AND discount ratio > 25 %
    #   (b) payment < $20 AND discount ratio > 50 %
    #   (c) in-store (RevenueCenterId = 1), duration < 10 min, traffic > 3,
    #       and (voids OR discounts)
    #   (d) any Gratuity < −$20, or ServiceCharge < 0, or any SoldPrice < −$20
    #   (e) opened between 2:00 AM and 7:00 AM
    df = (df
        .withColumn("is_fraud_high_value_excess_discount",
            (col("total_payment_amount") > 100.0) & (col("discount_ratio") > 0.25))
        .withColumn("is_fraud_low_value_excess_discount",
            (col("total_payment_amount") < 20.0) & (col("discount_ratio") > 0.50))
        .withColumn("is_fraud_suspicious_in_store",
            (col("RevenueCenterId") == 1) &
            (col("check_duration_minutes") < 10) &
            (col("TrafficCount") > 3) &
            (col("has_voids") | col("has_discounts")))
        .withColumn("is_fraud_negative_values",
            col("has_negative_gratuity") |
            (col("ServiceCharge") < 0) |
            col("has_negative_item_price"))
        .withColumn("is_fraud_irregular_hours",
            (col("open_hour") >= 2) & (col("open_hour") < 7))
    )

    # Column cleanup (legacy section 1h): drop all-null LoyaltyId; nothing to fill
    df = drop_all_null_columns(df, "checks")
    df = fill_numeric_nulls(df, "checks")

    return df


# ---------------------------------------------------------------------------
# Employee discount summary (legacy section 2a) — fraud flag (f) input
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.employee_discount_summary",
    comment=(
        "Per-employee total discount amount and discount count, exploded from "
        "the raw bronze checks Discounts array."
    ),
)
def employee_discount_summary():
    checks_bronze = spark.read.table(f"{BRONZE_SCHEMA}.checks")
    return (checks_bronze
        .select(explode(col("Discounts")).alias("d"))
        .filter(col("d.EmployeeId").isNotNull())
        .groupBy(col("d.EmployeeId").alias("EmployeeId"))
        .agg(
            _sum("d.Amount").alias("total_discount_amount"),
            _count(lit(1)).alias("discount_count"),
        ))


# ---------------------------------------------------------------------------
# checks — the main Silver fact table (legacy sections 1c-1h + flag f)
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.checks",
    comment=(
        "Cleaned, deduplicated, typed and fraud-flagged checks fact table "
        "(hypotheses a-f) deduplicated on (LocationId, BusinessDate, Id)."
    ),
)
@dp.expect_or_drop(
    "valid_primary_keys",
    "LocationId IS NOT NULL AND BusinessDate IS NOT NULL AND Id IS NOT NULL",
)
def checks():
    base = spark.read.table("silver_checks_base")

    # Hypothesis (f): employees in the top 5 % by total discount amount
    # (95th-percentile threshold). The legacy notebook collected the
    # threshold and top employee IDs with .collect() and an IN-list; here
    # the same result is computed declaratively as a single-row array and
    # joined in, so no action is needed.
    emp = spark.read.table(f"{SILVER_SCHEMA}.employee_discount_summary")
    threshold = emp.agg(percentile_approx("total_discount_amount", 0.95).alias("threshold"))
    top_employees = (
        emp.crossJoin(threshold)
        .where(col("total_discount_amount") >= col("threshold"))
        .agg(collect_set("EmployeeId").alias("top_employee_ids"))
    )

    # Flag every check that has any discount applied by a top-5 % employee
    return (base.crossJoin(top_employees)
        .withColumn(
            "is_fraud_top_discount_employee",
            expr("exists(coalesce(Discounts, array()), x -> array_contains(top_employee_ids, x.EmployeeId))"),
        )
        .drop("top_employee_ids"))


# ---------------------------------------------------------------------------
# suspect_fraud_checks — checks where at least one flag (a-f) is true
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{SILVER_SCHEMA}.suspect_fraud_checks",
    comment="Only the Silver checks where at least one fraud flag (a-f) is true.",
)
def suspect_fraud_checks():
    checks = spark.read.table(f"{SILVER_SCHEMA}.checks")
    fraud_condition = " OR ".join(f"{c} = true" for c in FRAUD_FLAG_COLS)
    return checks.where(fraud_condition)


# ---------------------------------------------------------------------------
# quarantine_bad_data — records with NULL primary keys (legacy section 1a)
# ---------------------------------------------------------------------------
@dp.table(
    name=f"{SILVER_SCHEMA}.quarantine_bad_data",
    comment=(
        "Raw bronze checks records with a NULL primary key (LocationId / "
        "BusinessDate / Id) — structurally invalid rows excluded from "
        "workspace.silver.checks."
    ),
)
@dp.expect(
    "is_quarantine_record",
    "LocationId IS NULL OR BusinessDate IS NULL OR Id IS NULL",
)
def quarantine_bad_data():
    checks_bronze = spark.readStream.table(f"{BRONZE_SCHEMA}.checks")
    return checks_bronze.where(
        col("LocationId").isNull()
        | col("BusinessDate").isNull()
        | col("Id").isNull()
    )


# ---------------------------------------------------------------------------
# Remaining bronze tables (legacy section 3): row-wise cleaning as streaming
# tables — type casts -> trim -> drop all-null -> fill numeric nulls.
# The 5 exploded child tables hold no NULL values in the current data; the 4
# dimension tables are empty (legacy helpers are no-ops on empty inputs), so
# their column sets are preserved unchanged.
# ---------------------------------------------------------------------------
def _cleaned_stream(table_name):
    df = spark.readStream.table(f"{BRONZE_SCHEMA}.{table_name}")
    return clean_bronze_table(df, table_name)


@dp.table(
    name=f"{SILVER_SCHEMA}.checks_discounts",
    comment="Cleaned per-discount rows exploded from the bronze checks Discounts array.",
)
def checks_discounts():
    return _cleaned_stream("checks_discounts")


@dp.table(
    name=f"{SILVER_SCHEMA}.checks_items",
    comment="Cleaned per-line-item rows exploded from the bronze checks ItemsSold array (nested Discounts kept as-is).",
)
def checks_items():
    return _cleaned_stream("checks_items")


@dp.table(
    name=f"{SILVER_SCHEMA}.checks_payments",
    comment="Cleaned per-payment rows exploded from the bronze checks Payments array.",
)
def checks_payments():
    return _cleaned_stream("checks_payments")


@dp.table(
    name=f"{SILVER_SCHEMA}.checks_tax",
    comment="Cleaned per-tax rows exploded from the bronze checks Tax array.",
)
def checks_tax():
    return _cleaned_stream("checks_tax")


@dp.table(
    name=f"{SILVER_SCHEMA}.checks_voids",
    comment="Cleaned per-void rows exploded from the bronze checks Voids array.",
)
def checks_voids():
    return _cleaned_stream("checks_voids")


@dp.table(
    name=f"{SILVER_SCHEMA}.employees",
    comment="Cleaned employee dimension (schema pinned in bronze; ingests incrementally once files land in rawlanding/employees).",
)
def employees():
    return _cleaned_stream("employees")


@dp.table(
    name=f"{SILVER_SCHEMA}.items_table",
    comment="Cleaned item dimension (schema pinned in bronze; ingests incrementally once files land in rawlanding/items).",
)
def items_table():
    return _cleaned_stream("items_table")


@dp.table(
    name=f"{SILVER_SCHEMA}.locations_with_coordinates",
    comment="Cleaned location dimension (schema pinned in bronze; ingests incrementally once files land in rawlanding/locations).",
)
def locations_with_coordinates():
    return _cleaned_stream("locations_with_coordinates")


@dp.table(
    name=f"{SILVER_SCHEMA}.revenue_centers",
    comment="Cleaned revenue-center dimension (schema pinned in bronze; ingests incrementally once files land in rawlanding/revenue_centers).",
)
def revenue_centers():
    return _cleaned_stream("revenue_centers")
