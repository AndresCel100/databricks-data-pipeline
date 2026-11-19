# ============================================================================
# Gold Layer — Fraud Analytics & Loss Prevention
# ----------------------------------------------------------------------------
# Lakeflow Spark Declarative Pipeline refactor of the legacy
# `Applebees_Pipeline_goldlayer` notebook.
#
# Reads the cleaned Silver tables from `workspace.silver` with batch
# `spark.read.table(...)` reads and publishes the four Gold deliverables to
# `workspace.gold` as materialized views, so the pipeline handles the joins,
# aggregations, and incremental refreshes automatically:
#   1. `gold_employee_fraud_scorecard`       — per-employee fraud scorecard
#   2. `gold_location_loss_prevention_summary` — per-location loss metrics
#   3. `gold_hourly_anomaly_trends`          — hourly anomaly patterns
#   4. `gold_suspect_check_details`          — receipt-level audit dataset
#
# The pipeline's default schema is `workspace.bronze`, so every Gold dataset
# is named with its full three-part Unity Catalog identifier
# (`workspace.gold.<table_name>`).
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, count as _count, sum as _sum, avg as _avg, explode,
)

SILVER_SCHEMA = "workspace.silver"
GOLD_SCHEMA = "workspace.gold"

# ---------------------------------------------------------------------------
# Table 1: gold_employee_fraud_scorecard
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_employee_fraud_scorecard",
    comment=(
        "Per-employee fraud scorecard: suspect-check count, per-hypothesis "
        "fraud flag totals, and discount/void dollars at risk, aggregated by "
        "employee and location. Employees are attributed by exploding the "
        "Discounts array and joining the employees dimension for the name."
    ),
    cluster_by=["LocationId"],
)
def gold_employee_fraud_scorecard():
    source = spark.read.table(f"{SILVER_SCHEMA}.suspect_fraud_checks")

    # Explode the Discounts array to attribute each suspect check to the
    # employee who applied a discount.
    exploded = (source
        .select(
            col("LocationId"),
            col("Id"),
            col("total_discount_amount"),
            col("total_void_amount"),
            col("is_fraud_high_value_excess_discount"),
            col("is_fraud_low_value_excess_discount"),
            col("is_fraud_irregular_hours"),
            col("is_fraud_negative_values"),
            col("is_fraud_suspicious_in_store"),
            explode(col("Discounts")).alias("d"),
        )
        .filter(col("d.EmployeeId").isNotNull())
        .select(
            col("d.EmployeeId").alias("employee_id"),
            col("LocationId"),
            col("Id"),
            col("total_discount_amount"),
            col("total_void_amount"),
            col("is_fraud_high_value_excess_discount"),
            col("is_fraud_low_value_excess_discount"),
            col("is_fraud_irregular_hours"),
            col("is_fraud_negative_values"),
            col("is_fraud_suspicious_in_store"),
        )
    )

    # Deduplicate to one row per (employee_id, LocationId, Id) so an employee
    # who applied multiple discounts on the same check is not double-counted.
    deduped = exploded.dropDuplicates(["employee_id", "LocationId", "Id"])

    # Join with the employees dimension for the human-readable employee name.
    emp_names = spark.read.table(f"{SILVER_SCHEMA}.employees").select(
        col("Id").alias("emp_id"),
        col("Name").alias("employee_name"),
    )

    # Aggregate fraud flags and financial risk per employee + location.
    return (deduped
        .join(emp_names, deduped.employee_id == emp_names.emp_id, "left")
        .drop("emp_id")
        .groupBy("employee_id", "employee_name", "LocationId")
        .agg(
            # Count of distinct suspect checks attributed to this employee
            _count("Id").alias("total_suspect_checks"),
            # Sum each fraud flag (cast boolean -> 0/1) per hypothesis
            _sum(col("is_fraud_high_value_excess_discount").cast("int")).alias("high_val_discount_flags"),
            _sum(col("is_fraud_low_value_excess_discount").cast("int")).alias("low_val_discount_flags"),
            _sum(col("is_fraud_irregular_hours").cast("int")).alias("irregular_hours_flags"),
            _sum(col("is_fraud_negative_values").cast("int")).alias("negative_price_flags"),
            _sum(col("is_fraud_suspicious_in_store").cast("int")).alias("suspicious_togo_flags"),
            # Financial exposure: discount and void dollars on suspect checks
            _sum("total_discount_amount").alias("total_discount_dollars_at_risk"),
            _sum("total_void_amount").alias("total_void_dollars_at_risk"),
        )
    )


# ---------------------------------------------------------------------------
# Table 2: gold_location_loss_prevention_summary
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_location_loss_prevention_summary",
    comment=(
        "Loss prevention summary by location and revenue center: flagged-check "
        "count plus discount, void, and combined dollars at risk on suspect "
        "checks, with revenue center names joined for readability."
    ),
    cluster_by=["LocationId"],
)
def gold_location_loss_prevention_summary():
    source = spark.read.table(f"{SILVER_SCHEMA}.suspect_fraud_checks")

    # Join with the revenue_centers dimension for the revenue center name.
    rc_names = spark.read.table(f"{SILVER_SCHEMA}.revenue_centers").select(
        col("Id").alias("rc_id"),
        col("Name").alias("revenue_center_name"),
    )

    # Aggregate loss metrics per location + revenue center.
    return (source
        .join(rc_names, source.RevenueCenterId == rc_names.rc_id, "left")
        .drop("rc_id")
        .groupBy("LocationId", "revenue_center_name")
        .agg(
            # Total suspect checks at this location + revenue center
            _count("Id").alias("total_flagged_checks"),
            # Total discount dollars at risk on suspect checks
            _sum("total_discount_amount").alias("total_discount_loss"),
            # Total void dollars at risk on suspect checks
            _sum("total_void_amount").alias("total_void_loss"),
            # Combined financial exposure: discounts + voids
            (_sum("total_discount_amount") + _sum("total_void_amount")).alias("total_dollars_at_risk"),
        )
    )


# ---------------------------------------------------------------------------
# Table 3: gold_hourly_anomaly_trends
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_hourly_anomaly_trends",
    comment=(
        "Hourly anomaly trends by location: suspect-check count, irregular-"
        "hours and suspicious in-store/togo flag counts, and average check "
        "duration per hour of day (from the silver open_hour column)."
    ),
    cluster_by=["LocationId"],
)
def gold_hourly_anomaly_trends():
    source = spark.read.table(f"{SILVER_SCHEMA}.suspect_fraud_checks")

    # Aggregate anomaly metrics per location + hour of day.
    return (source
        .groupBy("LocationId", col("open_hour").alias("hour_of_day"))
        .agg(
            # Total suspect checks opened in this hour
            _count("Id").alias("suspect_check_count"),
            # How many triggered the irregular-hours flag (2-7 AM)
            _sum(col("is_fraud_irregular_hours").cast("int")).alias("off_hours_count"),
            # How many triggered the suspicious in-store / togo flag
            _sum(col("is_fraud_suspicious_in_store").cast("int")).alias("suspicious_togo_count"),
            # Average check duration for context
            _avg("check_duration_minutes").alias("avg_check_duration_minutes"),
        )
    )


# ---------------------------------------------------------------------------
# Table 4: gold_suspect_check_details
# ---------------------------------------------------------------------------
# The legacy notebook created a UC view over silver suspect_fraud_checks;
# persistent UC views are not available in Python pipelines, so this is a
# materialized view that exposes every column 1:1 for receipt-level audit.
@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.gold_suspect_check_details",
    comment=(
        "Receipt-level audit dataset: every column of the silver "
        "suspect_fraud_checks table, exposed in the gold schema for detailed "
        "fraud investigation."
    ),
)
def gold_suspect_check_details():
    return spark.read.table(f"{SILVER_SCHEMA}.suspect_fraud_checks")
