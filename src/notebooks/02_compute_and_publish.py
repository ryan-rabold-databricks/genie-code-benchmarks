# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Compute unit costs and publish benchmarks
# MAGIC
# MAGIC 1. **Attribute** Genie Code DBUs, cost, and turns to each logged run (`v_benchmark_run_costs`) and give
# MAGIC    every run a status. Only `valid` runs count. Cost is priced from the system tables (account price,
# MAGIC    else list price); free-allowance usage is priced at the workspace's paid Genie Code rate.
# MAGIC 2. **Aggregate** engineer runs into a candidate per workload, config, skill version, and effort.
# MAGIC    DBUs, cost, and turns per unit are the **median** of per-run values, so one outlier run cannot move them.
# MAGIC    Business-user runs on the same config produce a `business_user_multiplier`.
# MAGIC 3. **Gate and publish**, per workload and effort level. All comparisons use **DBUs per unit**, so price
# MAGIC    changes never look like re-measurements or flip a decision; dollars are for reporting.
# MAGIC    * Candidates need `min_runs` valid runs. Runs that disagree (per-run DBUs per unit spread above
# MAGIC      `max_spread_pct`) need one more, so the median is not set by an outlier.
# MAGIC    * Published benchmarks (baseline or released skill, current or not) that were re-measured are
# MAGIC      **refreshed first**, so every comparison below uses fresh numbers. This is how re-baselining works.
# MAGIC    * First baseline: published (measure first). A skill version with no baseline: skipped.
# MAGIC    * A new skill version is published (release the skill) only if it uses at least `min_improvement_pct`
# MAGIC      fewer DBUs per unit than the cheapest published benchmark; otherwise it is rejected.
# MAGIC    * Current changes only when another published benchmark is at least `min_improvement_pct` cheaper than
# MAGIC      the current one (`CURRENT_CHANGED`), so run-to-run noise cannot flip it back and forth.
# MAGIC
# MAGIC Idempotent: re-running with the same runs produces the same tables and adds no decision rows.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
if not dbutils.widgets.get("catalog"):
    raise ValueError("Set the catalog widget (job parameter 'catalog') to the catalog that holds the benchmark schema")
dbutils.widgets.text("schema", "genie_code_benchmarks")
dbutils.widgets.text("billing_lag_hours", "6")
dbutils.widgets.text("min_runs", "2")
dbutils.widgets.text("max_spread_pct", "25")
dbutils.widgets.text("min_improvement_pct", "10")

fq = f"`{dbutils.widgets.get('catalog')}`.`{dbutils.widgets.get('schema')}`"
lag = float(dbutils.widgets.get("billing_lag_hours"))
min_runs = int(dbutils.widgets.get("min_runs"))
max_spread_pct = float(dbutils.widgets.get("max_spread_pct"))
min_improvement_pct = float(dbutils.widgets.get("min_improvement_pct"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Per-run attribution and status

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW run_results AS
SELECT run_id, workload_id, config, skill_version, effort, runner_type, run_by, start_ts, end_ts,
       artifacts_produced, dbus, turns, noise_turns, overlapping_runs, cost_usd, free_dbus, price_source,
       CASE
         WHEN overlapping_runs > 0                                     THEN 'excluded_overlap'
         WHEN hours_since_end < {lag}                                  THEN 'pending_billing'
         WHEN dbus IS NULL                                             THEN 'no_billing'
         WHEN cost_usd IS NULL OR unpriced_rows > 0                    THEN 'no_price'
         WHEN noise_turns > 0                                          THEN 'excluded_noise'
         WHEN coalesce(artifacts_produced, 0) < 1                      THEN 'excluded_no_artifacts'
         ELSE 'valid'
       END AS status,
       current_timestamp() AS computed_at
FROM {fq}.v_benchmark_run_costs
""")
# Rebuilt each run (it is a snapshot), so column changes never break an overwrite
spark.sql(f"CREATE OR REPLACE TABLE {fq}.benchmark_run_results "
          f"COMMENT 'Per-run attribution snapshot with status (valid, pending_billing, no_billing, no_price, excluded_*)' "
          f"AS SELECT * FROM run_results")
display(spark.sql(f"SELECT status, count(*) runs FROM {fq}.benchmark_run_results GROUP BY status ORDER BY runs DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Candidate unit costs

# COMMAND ----------

candidates = spark.sql(f"""
WITH ok AS (SELECT * FROM {fq}.benchmark_run_results WHERE status = 'valid'),
-- Unit metrics are the MEDIAN of per-run values, so one outlier run cannot move the benchmark
eng AS (
  SELECT workload_id, config, skill_version, effort,
         count(*) AS n_runs, avg(cost_usd) AS avg_cost_per_run, sum(artifacts_produced) AS units,
         percentile(dbus     / nullif(artifacts_produced, 0), 0.5) AS dbus_per_unit,
         percentile(cost_usd / nullif(artifacts_produced, 0), 0.5) AS cost_per_unit_usd,
         percentile(turns    / nullif(artifacts_produced, 0), 0.5) AS turns_per_unit,
         min(dbus / nullif(artifacts_produced, 0)) AS min_run_dbus_per_unit,
         max(dbus / nullif(artifacts_produced, 0)) AS max_run_dbus_per_unit,
         min(cost_usd / nullif(artifacts_produced, 0)) AS min_run_cost_per_unit,
         max(cost_usd / nullif(artifacts_produced, 0)) AS max_run_cost_per_unit,
         CASE WHEN count(DISTINCT price_source) = 1 THEN max(price_source) ELSE 'mixed' END AS price_source
  FROM ok WHERE runner_type = 'engineer'
  GROUP BY ALL
),
bu AS (
  SELECT workload_id, config, skill_version, effort,
         percentile(dbus / nullif(artifacts_produced, 0), 0.5) AS bu_dbus_per_unit
  FROM ok WHERE runner_type = 'business_user'
  GROUP BY ALL
)
SELECT e.workload_id, e.config, e.skill_version, e.effort, e.n_runs, e.avg_cost_per_run,
       e.cost_per_unit_usd, e.dbus_per_unit, e.turns_per_unit,
       e.min_run_cost_per_unit, e.max_run_cost_per_unit,
       (e.max_run_dbus_per_unit - e.min_run_dbus_per_unit) / nullif(e.dbus_per_unit, 0) * 100 AS spread_pct,
       100.0 / nullif(e.cost_per_unit_usd, 0) AS units_per_100_usd,
       b.bu_dbus_per_unit / nullif(e.dbus_per_unit, 0) AS business_user_multiplier,
       e.price_source
FROM eng e
LEFT JOIN bu b
  ON e.workload_id = b.workload_id AND e.config = b.config AND e.effort = b.effort
 AND e.skill_version <=> b.skill_version
WHERE e.units > 0
""").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gate and publish

# COMMAND ----------

def bid(c):
    return f"{c['workload_id']}|{c['config']}|{c['skill_version'] or '-'}|{c['effort']}"

METRICS = ["n_runs", "avg_cost_per_run", "cost_per_unit_usd", "min_run_cost_per_unit", "max_run_cost_per_unit", "spread_pct",
           "dbus_per_unit", "turns_per_unit", "units_per_100_usd", "business_user_multiplier"]
FIELDS = METRICS + ["price_source"]

def label(r):
    return f"{r['config']} {r['skill_version']}" if r.get("skill_version") else r["config"]

def same(a, b):
    if isinstance(a, str) or isinstance(b, str):
        return a == b
    return (a is None and b is None) or (a is not None and b is not None and abs(a - b) <= 1e-9)

def pct_change(new, ref):
    return (new - ref) / ref * 100 if ref else None  # ref 0: no % change

def dbu(r):
    return r["dbus_per_unit"]

def decide(c, decision, reason, ref=None, delta=None):
    return {"candidate_id": bid(c), "workload_id": c["workload_id"], "effort": c["effort"], "candidate_config": c["config"],
            "candidate_skill_version": c["skill_version"], "candidate_n_runs": c["n_runs"],
            "candidate_cost_per_unit": c["cost_per_unit_usd"], "current_config": ref["config"] if ref else None,
            "current_cost_per_unit": ref["cost_per_unit_usd"] if ref else None, "delta_pct": delta,
            "decision": decision, "reason": reason}

def not_ready(c):
    """Decision if the candidate does not have enough consistent runs yet, else None."""
    if c["n_runs"] < min_runs:
        return decide(c, "SKIP_INSUFFICIENT_RUNS", f"{c['n_runs']} valid runs; need {min_runs}")
    if (c["spread_pct"] or 0) > max_spread_pct and c["n_runs"] < min_runs + 1:
        return decide(c, "SKIP_HIGH_SPREAD", f"Runs differ by {c['spread_pct']:.0f}% of DBUs per unit (limit {max_spread_pct:.0f}%); "
                                             f"log {min_runs + 1 - c['n_runs']} more run(s)")
    return None

def units(r):
    return f"{dbu(r):.3f} DBUs (${r['cost_per_unit_usd']:.3f}) per unit"

existing = {r["benchmark_id"]: r.asDict() for r in spark.table(f"{fq}.unit_cost_benchmarks").collect()}
writes = {}     # benchmark_id -> row to upsert; "_metrics_changed" marks re-measured rows
decisions = []

groups = {}
for c in candidates:
    groups.setdefault((c["workload_id"], c["effort"]), []).append(c.asDict())

for key, cands in groups.items():
    # Published benchmarks for this workload and effort: the baseline plus every released skill version.
    pub = {b: dict(r) for b, r in existing.items() if (r["workload_id"], r["effort"]) == key}
    prev_current = next((b for b, r in pub.items() if r["is_current"]), None)
    published_now = set()
    refreshed = set()

    # 1. Refresh published benchmarks (current or superseded) that were re-measured, so later comparisons use fresh numbers
    new_cands = []
    for c in cands:
        cid = bid(c)
        if cid not in pub:
            new_cands.append(c)
            continue
        old = pub[cid]
        changed = [k for k in FIELDS if not same(old.get(k), c[k])]
        if not changed:
            continue
        skip = not_ready(c)
        if skip:
            decisions.append(skip)  # keep the last published numbers until the new runs are sufficient
            continue
        decisions.append(decide(c, "REFRESHED", f"Re-measured: {old['n_runs']} -> {c['n_runs']} engineer runs, DBUs per unit "
                                                f"{dbu(old):.3f} -> {dbu(c):.3f} (changed: {', '.join(changed)})",
                                ref=old, delta=pct_change(dbu(c), dbu(old))))
        pub[cid] = {**old, **{k: c[k] for k in FIELDS}}
        writes[cid] = {**pub[cid], "_metrics_changed": True}
        refreshed.add(cid)

    baseline_id = f"{key[0]}|baseline|-|{key[1]}"

    # 2. First baseline
    for c in [c for c in new_cands if c["config"] == "baseline"]:
        skip = not_ready(c)
        if skip:
            decisions.append(skip)
            continue
        cid = bid(c)
        pub[cid] = {**c, "benchmark_id": cid, "is_current": False}
        writes[cid] = {**pub[cid], "_metrics_changed": True}
        published_now.add(cid)
        decisions.append(decide(c, "PUBLISH_BASELINE", "First measurement for this workload"))

    # 3. New skill versions, cheapest first, each compared with the cheapest published benchmark
    for c in sorted([c for c in new_cands if c["config"] != "baseline"], key=dbu):
        skip = not_ready(c)
        if skip:
            decisions.append(skip)
            continue
        if baseline_id not in pub:
            decisions.append(decide(c, "SKIP_NO_BASELINE", "Measure the baseline before testing a skill"))
            continue
        best = min(pub.values(), key=dbu)
        delta = pct_change(dbu(c), dbu(best))
        if delta is not None and delta <= -min_improvement_pct:
            cid = bid(c)
            pub[cid] = {**c, "benchmark_id": cid, "is_current": False}
            writes[cid] = {**pub[cid], "_metrics_changed": True}
            published_now.add(cid)
            decisions.append(decide(c, "PUBLISH_IMPROVED", f"Uses {-delta:.1f}% fewer DBUs per unit than {label(best)} "
                                                           f"({units(c)}): release the skill", ref=best, delta=delta))
        else:
            change = f"{delta:+.1f}%" if delta is not None else "not lower than 0"
            decisions.append(decide(c, "REJECT_NO_IMPROVEMENT", f"DBUs per unit {change} vs {label(best)} "
                                                                 f"(needs at least {min_improvement_pct:.0f}% fewer): do not release",
                                    ref=best, delta=delta))

    # 4. Current. A newly released skill becomes current. Otherwise current moves only when another published
    #    benchmark is at least min_improvement_pct cheaper than the current one (hysteresis against noise).
    if not pub:
        continue
    cheapest = min(pub, key=lambda b: (dbu(pub[b]), b))
    new_current = cheapest
    if prev_current in pub and cheapest != prev_current and cheapest not in published_now:
        gap = pct_change(dbu(pub[cheapest]), dbu(pub[prev_current]))
        if gap is None or gap > -min_improvement_pct:
            new_current = prev_current
    for b, r in pub.items():
        want = b == new_current
        if bool(r["is_current"]) != want or b in writes:
            writes[b] = {**writes.get(b, {**r, "_metrics_changed": False}), "is_current": want}
    if prev_current and new_current != prev_current and new_current not in published_now:
        old_r, new_r = pub[prev_current], pub[new_current]
        moved = [label(x) + " re-measured" for b, x in ((prev_current, old_r), (new_current, new_r)) if b in refreshed]
        why = " and ".join(moved) if moved else "re-evaluated"
        delta = pct_change(dbu(new_r), dbu(old_r))
        decisions.append(decide(new_r, "CURRENT_CHANGED",
                                f"{why}: {label(new_r)} at {dbu(new_r):.3f} DBUs per unit is {-delta:.1f}% below {label(old_r)} "
                                f"at {dbu(old_r):.3f}, so {label(new_r)} is current", ref=old_r, delta=delta))

# COMMAND ----------

pub_cols = ["benchmark_id", "workload_id", "config", "skill_version", "effort"] + FIELDS + ["is_current", "_metrics_changed"]
pub_schema = ("benchmark_id string, workload_id string, config string, skill_version string, effort string, n_runs bigint, "
              "avg_cost_per_run double, cost_per_unit_usd double, min_run_cost_per_unit double, max_run_cost_per_unit double, "
              "spread_pct double, dbus_per_unit double, turns_per_unit double, units_per_100_usd double, "
              "business_user_multiplier double, price_source string, is_current boolean, metrics_changed boolean")
if writes:
    spark.createDataFrame([tuple({**r, "benchmark_id": b}.get(k) for k in pub_cols) for b, r in writes.items()], pub_schema) \
        .createOrReplaceTempView("to_publish")
    spark.sql(f"""
    MERGE INTO {fq}.unit_cost_benchmarks t USING to_publish s ON t.benchmark_id = s.benchmark_id
    WHEN MATCHED THEN UPDATE SET n_runs = s.n_runs, avg_cost_per_run = s.avg_cost_per_run,
      cost_per_unit_usd = s.cost_per_unit_usd, min_run_cost_per_unit = s.min_run_cost_per_unit,
      max_run_cost_per_unit = s.max_run_cost_per_unit, spread_pct = s.spread_pct, dbus_per_unit = s.dbus_per_unit,
      turns_per_unit = s.turns_per_unit, units_per_100_usd = s.units_per_100_usd,
      business_user_multiplier = s.business_user_multiplier, price_source = s.price_source,
      published_at  = CASE WHEN s.metrics_changed THEN current_timestamp() ELSE t.published_at END,
      is_current = s.is_current,
      -- superseded_at marks the moment a current benchmark stopped being current; never-current rows stay NULL
      superseded_at = CASE WHEN s.is_current THEN NULL WHEN t.is_current THEN current_timestamp() ELSE t.superseded_at END
    WHEN NOT MATCHED THEN INSERT (benchmark_id, workload_id, config, skill_version, effort, n_runs, avg_cost_per_run,
      cost_per_unit_usd, min_run_cost_per_unit, max_run_cost_per_unit, spread_pct, dbus_per_unit, turns_per_unit,
      units_per_100_usd, business_user_multiplier, price_source, published_at, is_current, superseded_at)
    VALUES (s.benchmark_id, s.workload_id, s.config, s.skill_version, s.effort, s.n_runs, s.avg_cost_per_run,
      s.cost_per_unit_usd, s.min_run_cost_per_unit, s.max_run_cost_per_unit, s.spread_pct, s.dbus_per_unit,
      s.turns_per_unit, s.units_per_100_usd, s.business_user_multiplier, s.price_source, current_timestamp(), s.is_current,
      NULL)
    """)

# COMMAND ----------

if decisions:
    dec_cols = ["candidate_id", "workload_id", "effort", "candidate_config", "candidate_skill_version", "candidate_n_runs",
                "candidate_cost_per_unit", "current_config", "current_cost_per_unit", "delta_pct", "decision", "reason"]
    dec_schema = ("candidate_id string, workload_id string, effort string, candidate_config string, candidate_skill_version string, "
                  "candidate_n_runs bigint, candidate_cost_per_unit double, current_config string, current_cost_per_unit double, "
                  "delta_pct double, decision string, reason string")
    spark.createDataFrame([tuple(d[k] for k in dec_cols) for d in decisions], dec_schema).createOrReplaceTempView("new_decisions")
    # Append-only audit trail: add a row only when it differs from the candidate's latest row (any decision), so
    # re-running the job with the same inputs adds nothing, a return to an earlier state is recorded, and earlier
    # decisions are never overwritten.
    spark.sql(f"""
    INSERT INTO {fq}.benchmark_decisions
      (decided_at, candidate_id, workload_id, effort, candidate_config, candidate_skill_version, candidate_n_runs,
       candidate_cost_per_unit, current_config, current_cost_per_unit, delta_pct, decision, reason)
    SELECT current_timestamp(), s.*
    FROM new_decisions s
    LEFT JOIN (
      SELECT * FROM {fq}.benchmark_decisions
      QUALIFY row_number() OVER (PARTITION BY candidate_id ORDER BY decided_at DESC, decision) = 1
    ) t ON t.candidate_id = s.candidate_id AND t.decision = s.decision
       AND t.candidate_n_runs = s.candidate_n_runs
       AND round(t.candidate_cost_per_unit, 6) <=> round(s.candidate_cost_per_unit, 6)
       AND t.current_config <=> s.current_config
       AND round(t.current_cost_per_unit, 6) <=> round(s.current_cost_per_unit, 6)
       AND round(t.delta_pct, 4) <=> round(s.delta_pct, 4)
       AND t.reason <=> s.reason
    WHERE t.candidate_id IS NULL
    """)

for d in decisions:
    print(f"{d['decision']:<24} {d['candidate_id']:<55} {d['reason']}")

display(spark.sql(f"SELECT * FROM {fq}.unit_cost_benchmarks WHERE is_current ORDER BY workload_id"))
