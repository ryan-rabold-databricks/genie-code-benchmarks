# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: Genie Code unit-cost benchmark tables
# MAGIC
# MAGIC Creates (idempotently) the schema, tables, and view used by the benchmark process:
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `benchmark_workloads` | The standard workloads we measure, each with a fixed prompt script and a unit of measure |
# MAGIC | `benchmark_runs` | **Build log, engineering only.** One row per scripted benchmark run (start/end time, runner, config) |
# MAGIC | `v_benchmark_run_costs` | Attributes Genie Code DBUs (billing) and turns (Genie Code events) to each run |
# MAGIC | `benchmark_run_results` | Snapshot of the view with a status per run (written by `02_compute_and_publish`) |
# MAGIC | `unit_cost_benchmarks` | Published, versioned unit costs. `is_current = true` is what the dashboard, budgets, and forecast skill read |
# MAGIC | `benchmark_decisions` | Audit trail: why each candidate was published or rejected |
# MAGIC
# MAGIC Safe to re-run: every statement is `IF NOT EXISTS` or a `MERGE`.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
if not dbutils.widgets.get("catalog"):
    raise ValueError("Set the catalog widget (job parameter 'catalog') to the catalog that holds the benchmark schema")
dbutils.widgets.text("schema", "genie_code_benchmarks")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq} COMMENT 'Genie Code unit-cost benchmarks: workloads, engineering build log, published unit costs'")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.benchmark_workloads (
  workload_id      STRING  NOT NULL COMMENT 'Stable key, e.g. add_widget',
  workload_name    STRING  COMMENT 'Display name',
  complexity       STRING  COMMENT 'Simple / Medium / Complex / Variable',
  unit_of_measure  STRING  COMMENT 'What one unit is, e.g. widget, dashboard, metric view',
  script_prompt    STRING  COMMENT 'Fixed prompt every benchmark run uses, so runs are comparable',
  is_active        BOOLEAN COMMENT 'Currently being benchmarked',
  updated_at       TIMESTAMP
) COMMENT 'Standard Genie Code workloads measured by the benchmark process'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.benchmark_runs (
  run_id             STRING  NOT NULL,
  workload_id        STRING  NOT NULL,
  config             STRING  COMMENT 'baseline, or the name of the skill under test',
  skill_version      STRING  COMMENT 'Skill version under test; baseline runs use the current workspace setup',
  effort             STRING  COMMENT 'Genie Code effort level: Auto or Low',
  runner_type        STRING  COMMENT 'engineer or business_user',
  run_by             STRING  COMMENT 'User who ran the build (must match billing run_as)',
  start_ts           TIMESTAMP,
  end_ts             TIMESTAMP,
  artifacts_produced INT     COMMENT 'Units produced, in the workload unit_of_measure',
  notes              STRING,
  logged_at          TIMESTAMP,
  workspace_id       STRING  COMMENT 'Workspace the run was logged in; billing and events are attributed from this workspace only'
) COMMENT 'Engineering-only build log for scripted benchmark runs. Everyday builds are never logged.'
""")

# One-time migrations for tables created by earlier versions (no-ops once applied)
if "workspace_id" not in {f.name for f in spark.table(f"{fq}.benchmark_runs").schema}:
    # Runs logged before workspace_id existed were logged in the workspace running this first migration
    from databricks.sdk import WorkspaceClient
    spark.sql(f"ALTER TABLE {fq}.benchmark_runs ADD COLUMNS (workspace_id STRING COMMENT 'Workspace the run was logged in')")
    spark.sql(f"UPDATE {fq}.benchmark_runs SET workspace_id = :ws WHERE workspace_id IS NULL",
              args={"ws": str(WorkspaceClient().get_workspace_id())})

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.benchmark_run_results (
  run_id STRING, workload_id STRING, config STRING, skill_version STRING, effort STRING, runner_type STRING,
  run_by STRING, start_ts TIMESTAMP, end_ts TIMESTAMP, artifacts_produced INT,
  dbus DOUBLE, turns BIGINT, noise_turns BIGINT, overlapping_runs BIGINT, cost_usd DOUBLE, free_dbus DOUBLE,
  price_source STRING, status STRING, computed_at TIMESTAMP
) COMMENT 'Per-run attribution snapshot with status (valid, pending_billing, no_billing, no_price, excluded_*)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.unit_cost_benchmarks (
  benchmark_id        STRING NOT NULL,
  workload_id         STRING,
  config              STRING,
  skill_version       STRING,
  effort              STRING,
  n_runs              BIGINT,
  avg_cost_per_run    DOUBLE COMMENT 'Cost of the most recent valid run (USD)',
  cost_per_unit_usd   DOUBLE,
  min_run_cost_per_unit DOUBLE COMMENT 'Cheapest of the last min_runs runs, cost per unit',
  max_run_cost_per_unit DOUBLE COMMENT 'Most expensive of the last min_runs runs, cost per unit',
  spread_pct          DOUBLE COMMENT '(max - min DBUs per unit over the last min_runs runs) / last run DBUs per unit * 100',
  dbus_per_unit       DOUBLE COMMENT 'DBUs per unit of the most recent valid run; the release gate compares this',
  turns_per_unit      DOUBLE,
  units_per_100_usd   DOUBLE,
  business_user_multiplier DOUBLE COMMENT 'Business-user DBUs per unit / engineer DBUs per unit (null if no business-user runs)',
  price_source        STRING COMMENT 'account, list, or mixed: where the cost prices came from',
  published_at        TIMESTAMP,
  is_current          BOOLEAN,
  superseded_at       TIMESTAMP
) COMMENT 'Published unit-cost benchmarks. Read is_current = true for budgets, dashboard, and forecasts.'
""")

if "price_source" not in {f.name for f in spark.table(f"{fq}.unit_cost_benchmarks").schema}:
    spark.sql(f"ALTER TABLE {fq}.unit_cost_benchmarks ADD COLUMNS (price_source STRING COMMENT 'account, list, or mixed')")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.benchmark_decisions (
  decided_at                TIMESTAMP,
  candidate_id              STRING,
  workload_id               STRING,
  effort                    STRING,
  candidate_config          STRING,
  candidate_skill_version   STRING,
  candidate_n_runs          BIGINT,
  candidate_cost_per_unit   DOUBLE,
  current_config            STRING,
  current_cost_per_unit     DOUBLE,
  delta_pct                 DOUBLE,
  decision                  STRING COMMENT 'PUBLISH_BASELINE, PUBLISH_IMPROVED, REFRESHED, CURRENT_CHANGED, REJECT_NO_IMPROVEMENT, SKIP_INSUFFICIENT_RUNS, SKIP_HIGH_SPREAD, SKIP_NO_BASELINE, SKIP_STALE_SKILL',
  reason                    STRING
) COMMENT 'Why each benchmark candidate was published or rejected'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed the standard workloads
# MAGIC The first three are active (benchmark these first). The rest are defined so the catalog of
# MAGIC workloads matches the benchmark plan, and can be switched on later.

# COMMAND ----------

workloads = [
    ("add_widget", "Add a widget to an existing dashboard", "Simple", "widget",
     "Add a bar chart widget to the dashboard '<benchmark dashboard>' showing total order value by order priority from samples.tpch.orders.", True),
    ("dashboard_from_scratch", "Create a dashboard from scratch", "Medium", "dashboard",
     "Create a one-page AI/BI dashboard on samples.tpch.orders with 3 KPIs (orders, revenue, avg order value), a monthly revenue trend, and revenue by order priority.", True),
    ("metric_view", "Create or extend a metric view", "Medium-Complex", "metric view",
     "Create a metric view on samples.tpch.orders with measures total revenue, order count, and avg order value, and dimensions order month, order priority, and order status.", True),
    ("single_visualization", "Create a single visualization", "Simple", "chart", None, False),
    ("update_formatting", "Update dashboard formatting", "Simple-Medium", "formatting request", None, False),
    ("multipage_dashboard", "Build a multipage KPI dashboard", "Complex", "dashboard", None, False),
    ("exploratory_analysis", "Perform exploratory analysis", "Variable", "analytical session", None, False),
    ("convert_legacy_dashboard", "Convert legacy dashboards", "Variable", "migrated artifact", None, False),
]
spark.createDataFrame(workloads, "workload_id string, workload_name string, complexity string, unit_of_measure string, script_prompt string, is_active boolean") \
    .createOrReplaceTempView("seed_workloads")

spark.sql(f"""
MERGE INTO {fq}.benchmark_workloads t
USING seed_workloads s ON t.workload_id = s.workload_id
WHEN MATCHED THEN UPDATE SET workload_name = s.workload_name, complexity = s.complexity,
  unit_of_measure = s.unit_of_measure, script_prompt = coalesce(t.script_prompt, s.script_prompt), updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (workload_id, workload_name, complexity, unit_of_measure, script_prompt, is_active, updated_at)
  VALUES (s.workload_id, s.workload_name, s.complexity, s.unit_of_measure, s.script_prompt, s.is_active, current_timestamp())
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attribution view
# MAGIC Billing rows are **per user, per hour**, so a run is charged everything that user consumed in
# MAGIC Genie Code during the clock hours its window touches. Two checks keep that honest:
# MAGIC * `noise_turns`: Genie Code messages by the same user in those hours but **outside** the run window
# MAGIC   (other work leaking into the bill).
# MAGIC * `overlapping_runs`: other benchmark runs by the same user sharing an hour (cannot be separated).
# MAGIC
# MAGIC **Pricing comes from the system tables.** Each billing row is priced at its usage time from
# MAGIC `system.billing.account_prices` (the account's own price), else `system.billing.list_prices`.
# MAGIC Free-allowance usage (`GENIE_FREE_USAGE`, which has no price) is real consumption, so it is priced at
# MAGIC the paid Genie Code SKU the same workspace bills (its regional rate). A run with any row that cannot be
# MAGIC priced is marked `no_price` by `02_compute_and_publish`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq}.v_benchmark_run_costs
COMMENT 'Per-run Genie Code DBUs, cost, and turns, attributed by user, workspace, and hour'
AS
WITH all_runs AS (
  -- Billing hours are UTC. Bucket on epoch seconds so the result does not depend on the session time zone.
  -- attr_end rounds the end up to the hour, so a run ending exactly on the hour is not charged an extra hour.
  -- Open runs count as running until now, so they still show up as overlaps.
  SELECT * EXCEPT (skill_version),
         CASE WHEN config = 'baseline' THEN NULL ELSE skill_version END              AS skill_version,
         timestamp_seconds(floor(unix_timestamp(start_ts) / 3600) * 3600)            AS attr_start,
         timestamp_seconds(ceil(unix_timestamp(coalesce(end_ts, current_timestamp())) / 3600) * 3600) AS attr_end
  FROM {fq}.benchmark_runs
),
runs AS (
  SELECT * FROM all_runs WHERE end_ts IS NOT NULL
),
billing_rows AS (
  SELECT r.run_id, u.workspace_id, u.cloud, u.sku_name, u.usage_unit, u.usage_quantity, u.usage_start_time
  FROM runs r
  JOIN system.billing.usage u
    ON u.billing_origin_product = 'GENIE'
   AND u.usage_metadata.genie.surface = 'GENIE_CODE'
   AND lower(u.identity_metadata.run_as) = lower(r.run_by)
   AND u.workspace_id = r.workspace_id
   AND u.usage_date BETWEEN date_sub(to_date(r.attr_start), 1) AND date_add(to_date(r.attr_end), 1)  -- pruning only; exact window below
   AND u.usage_start_time >= r.attr_start
   AND u.usage_end_time   <= r.attr_end
),
paid_sku AS (
  -- The paid Genie Code SKU each workspace bills (one per region). Free-allowance usage is priced at it.
  SELECT workspace_id, max_by(sku_name, usage_start_time) AS paid_sku_name
  FROM system.billing.usage
  WHERE billing_origin_product = 'GENIE' AND usage_metadata.genie.surface = 'GENIE_CODE'
    AND sku_name <> 'GENIE_FREE_USAGE'
    AND workspace_id IN (SELECT DISTINCT workspace_id FROM runs)
  GROUP BY workspace_id
),
priced AS (
  SELECT b.run_id, b.usage_quantity,
         b.sku_name = 'GENIE_FREE_USAGE'                                  AS is_free,
         coalesce(ap.pricing.default, lp.pricing.effective_list.default)  AS price,
         CASE WHEN ap.pricing.default IS NOT NULL THEN 'account'
              WHEN lp.pricing.effective_list.default IS NOT NULL THEN 'list' END AS price_source
  FROM (SELECT b.*, CASE WHEN b.sku_name = 'GENIE_FREE_USAGE' THEN p.paid_sku_name ELSE b.sku_name END AS priced_sku
        FROM billing_rows b LEFT JOIN paid_sku p USING (workspace_id)) b
  LEFT JOIN system.billing.account_prices ap
    ON ap.sku_name = b.priced_sku AND ap.cloud = b.cloud AND ap.usage_unit = b.usage_unit AND ap.currency_code = 'USD'
   AND b.usage_start_time >= ap.price_start_time AND (ap.price_end_time IS NULL OR b.usage_start_time < ap.price_end_time)
  LEFT JOIN system.billing.list_prices lp
    ON lp.sku_name = b.priced_sku AND lp.cloud = b.cloud AND lp.usage_unit = b.usage_unit AND lp.currency_code = 'USD'
   AND b.usage_start_time >= lp.price_start_time AND (lp.price_end_time IS NULL OR b.usage_start_time < lp.price_end_time)
),
billing AS (
  SELECT run_id,
         sum(usage_quantity)                                   AS dbus,
         sum(usage_quantity * price)                           AS cost_usd,
         sum(CASE WHEN is_free THEN usage_quantity ELSE 0 END) AS free_dbus,
         count_if(price IS NULL)                               AS unpriced_rows,
         CASE WHEN count(DISTINCT price_source) = 1 THEN max(price_source)
              WHEN count(DISTINCT price_source) > 1 THEN 'mixed' END AS price_source
  FROM priced
  GROUP BY run_id
),
events AS (
  SELECT r.run_id,
         count_if(e.event_time BETWEEN r.start_ts AND r.end_ts)  AS turns,
         count_if(e.event_time < r.start_ts OR e.event_time > r.end_ts) AS noise_turns
  FROM runs r
  JOIN system.access.assistant_events e
    ON lower(e.initiated_by) = lower(r.run_by)
   AND e.workspace_id = r.workspace_id
   AND e.event_date BETWEEN date_sub(to_date(r.attr_start), 1) AND date_add(to_date(r.attr_end), 1)
   AND e.event_time >= r.attr_start
   AND e.event_time <  r.attr_end
  GROUP BY r.run_id
),
overlap AS (
  SELECT a.run_id, count(b.run_id) AS overlapping_runs
  FROM runs a
  JOIN all_runs b
    ON lower(a.run_by) = lower(b.run_by) AND a.run_id <> b.run_id AND a.workspace_id <=> b.workspace_id
   AND a.attr_start < b.attr_end AND b.attr_start < a.attr_end
  GROUP BY a.run_id
)
SELECT r.run_id, r.workload_id, r.config, r.skill_version, r.effort, r.runner_type, r.run_by,
       r.start_ts, r.end_ts, r.artifacts_produced,
       b.dbus, CAST(b.cost_usd AS DOUBLE) AS cost_usd, b.free_dbus, b.price_source, coalesce(b.unpriced_rows, 0) AS unpriced_rows,
       coalesce(e.turns, 0)            AS turns,
       coalesce(e.noise_turns, 0)      AS noise_turns,
       coalesce(o.overlapping_runs, 0) AS overlapping_runs,
       -- Lag is measured from the end of the last attributed billing hour, not the run end
       (unix_timestamp(current_timestamp()) - unix_timestamp(r.attr_end)) / 3600.0 AS hours_since_end
FROM runs r
LEFT JOIN billing b USING (run_id)
LEFT JOIN events  e USING (run_id)
LEFT JOIN overlap o USING (run_id)
""")

display(spark.sql(f"SHOW TABLES IN {fq}"))
