# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Log a benchmark run (engineering only)
# MAGIC
# MAGIC Use this notebook to bracket a **scripted** Genie Code benchmark run:
# MAGIC
# MAGIC 1. Set the widgets, choose `action = start`, run all. A `run_id` is recorded with the start time.
# MAGIC 2. In Genie Code, paste the workload's `script_prompt` (shown below) and complete the build.
# MAGIC    Do **not** use Genie Code for anything else until the run ends.
# MAGIC 3. Set `action = end`, enter `artifacts_produced`, run all.
# MAGIC
# MAGIC **Rules for clean attribution** (billing is per user, per hour):
# MAGIC * One run per user per clock hour. Start runs near the top of an hour and finish inside it when possible.
# MAGIC * No other Genie Code activity by the runner during the hours the run touches.
# MAGIC * Billing lands a few hours later; `02_compute_and_publish` marks runs `pending_billing` until then.
# MAGIC
# MAGIC Everyday builds are never logged here. This log exists only to measure unit costs.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
if not dbutils.widgets.get("catalog"):
    raise ValueError("Set the catalog widget (job parameter 'catalog') to the catalog that holds the benchmark schema")
dbutils.widgets.text("schema", "genie_code_benchmarks")
dbutils.widgets.dropdown("action", "start", ["start", "end", "cancel"])
dbutils.widgets.text("workload_id", "add_widget")
dbutils.widgets.text("config", "baseline")
dbutils.widgets.text("skill_version", "")
dbutils.widgets.dropdown("effort", "Auto", ["Auto", "Low"])
dbutils.widgets.dropdown("runner_type", "engineer", ["engineer", "business_user"])
dbutils.widgets.text("run_by", "")
dbutils.widgets.text("artifacts_produced", "1")
dbutils.widgets.text("notes", "")

w = {k: dbutils.widgets.get(k).strip() for k in
     ["catalog", "schema", "action", "workload_id", "config", "skill_version", "effort", "runner_type", "run_by", "artifacts_produced", "notes"]}
fq = f"`{w['catalog']}`.`{w['schema']}`"
# Emails are matched case-insensitively against billing and Genie Code events, so store them lowercase
run_by = (w["run_by"] or spark.sql("SELECT current_user()").first()[0]).lower()
# "baseline" in any case means the baseline; skill configs need a version so runs of one skill are not split
config = "baseline" if w["config"].lower() in ("", "baseline") else w["config"]
skill_version = None if config == "baseline" else (w["skill_version"] or None)

# COMMAND ----------

open_runs = spark.sql(f"""
  SELECT run_id, workload_id, config, start_ts FROM {fq}.benchmark_runs
  WHERE lower(run_by) = :run_by AND end_ts IS NULL
""", args={"run_by": run_by}).collect()

if w["action"] == "start":
    if open_runs:
        raise ValueError(f"{run_by} already has an open run {open_runs[0]['run_id']} ({open_runs[0]['workload_id']}). End or cancel it first.")
    wl = spark.sql(f"SELECT * FROM {fq}.benchmark_workloads WHERE workload_id = :w", args={"w": w["workload_id"]}).collect()
    if not wl:
        raise ValueError(f"Unknown workload_id '{w['workload_id']}'. See {w['catalog']}.{w['schema']}.benchmark_workloads.")
    if not wl[0]["is_active"] or not wl[0]["script_prompt"]:
        raise ValueError(f"Workload '{w['workload_id']}' is not active or has no script_prompt. Set is_active and a fixed "
                         f"script_prompt in benchmark_workloads first, so every run uses the same prompt.")
    if config != "baseline" and not skill_version:
        raise ValueError(f"Set skill_version for skill config '{config}' so its runs are grouped under one version.")
    if config == "baseline" and w["skill_version"]:
        print(f"Note: skill_version '{w['skill_version']}' ignored for a baseline run.")
    same_hour = spark.sql(f"""
      SELECT run_id FROM {fq}.benchmark_runs
      WHERE lower(run_by) = :run_by
        AND floor(unix_timestamp(end_ts) / 3600) = floor(unix_timestamp(current_timestamp()) / 3600)  -- same UTC billing hour
    """, args={"run_by": run_by}).collect()
    if same_hour:
        raise ValueError(f"{run_by} already ran a benchmark this UTC hour ({same_hour[0]['run_id']}). Starting another would "
                         "exclude both as overlapping. Wait for the next clock hour.")
    from databricks.sdk import WorkspaceClient
    run_id = spark.sql("SELECT uuid()").first()[0]
    spark.sql(f"""
      INSERT INTO {fq}.benchmark_runs
        (run_id, workload_id, config, skill_version, effort, runner_type, run_by, start_ts, notes, logged_at, workspace_id)
      VALUES (:run_id, :workload_id, :config, :skill_version, :effort, :runner_type, :run_by, current_timestamp(), :notes,
              current_timestamp(), :workspace_id)
    """, args={"run_id": run_id, "workload_id": w["workload_id"], "config": config,
               "skill_version": skill_version, "effort": w["effort"],
               "runner_type": w["runner_type"], "run_by": run_by, "notes": w["notes"] or None,
               "workspace_id": str(WorkspaceClient().get_workspace_id())})
    print(f"Started run {run_id} for {run_by}: {w['workload_id']} / {config} / {w['effort']}")
    print("\nPaste this prompt into Genie Code:\n")
    print(wl[0]["script_prompt"] or "(no script_prompt defined for this workload yet)")

elif w["action"] == "end":
    if not open_runs:
        raise ValueError(f"No open run for {run_by}.")
    artifacts = int(w["artifacts_produced"])
    if artifacts < 1:
        raise ValueError("artifacts_produced must be >= 1 (units in the workload's unit_of_measure).")
    run_id = open_runs[0]["run_id"]
    spark.sql(f"""
      UPDATE {fq}.benchmark_runs
      SET end_ts = current_timestamp(), artifacts_produced = :a,
          notes = coalesce(:notes, notes)
      WHERE run_id = :run_id
    """, args={"a": artifacts, "notes": w["notes"] or None, "run_id": run_id})
    print(f"Ended run {run_id}. Billing usually lands within a few hours; then run the compute job.")

elif w["action"] == "cancel":
    if not open_runs:
        raise ValueError(f"No open run for {run_by}.")
    spark.sql(f"DELETE FROM {fq}.benchmark_runs WHERE run_id = :run_id", args={"run_id": open_runs[0]["run_id"]})
    print(f"Cancelled and removed run {open_runs[0]['run_id']}.")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {fq}.benchmark_runs WHERE run_by = :run_by ORDER BY start_ts DESC LIMIT 10", args={"run_by": run_by}))
