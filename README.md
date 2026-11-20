# Genie Code Unit-Cost Benchmarks

A deployable reference implementation of the **unit-cost benchmark process** for Genie Code: measure what a
standard workload (add a widget, build a dashboard, build a metric view) costs today, test a workload skill
against that baseline, and publish a new unit cost only when the skill lowers it. Published unit costs feed team
budgets ("what $100 buys"), the Genie Cost Dashboard, and a large-build forecast skill.

It covers the first two phases of a Genie Code cost program: measure first, then optimize.

## Why it works this way

* **Genie Code billing has no per-artifact or per-session tag.** `system.billing.usage` records Genie Code DBUs
  per user, per hour (`usage_metadata.genie.surface = 'GENIE_CODE'`). So cost per artifact can only be measured by
  running **scripted builds in isolated user-hours** and dividing cost by units produced. Billing and events are
  filtered to the workspace the run was logged in, so the runner's Genie Code use in other workspaces is not counted.
* **Unit cost is the most recent valid run's** DBUs, cost, and turns per unit, so a re-measurement (for example
  after a Genie Code model change) replaces the old number instead of being averaged with older runs. Earlier runs
  are used only as a consistency check: a candidate needs `min_runs` valid runs, and its last `min_runs` runs must
  agree within `max_spread_pct`, so one outlier run is never published.
* **Turns** come from `system.access.assistant_events` (one row per user message).
* **Prices come from the system tables.** Each billing row is priced at its usage time from
  `system.billing.account_prices` (your account's price), else `system.billing.list_prices`; `price_source` on each
  run and benchmark says which. **Free-allowance usage** (`GENIE_FREE_USAGE`) has no price but is real consumption,
  so it is priced at the paid Genie Code SKU the same workspace bills (its regional rate), and reported separately
  as `free_dbus`.
* **The release gate compares DBUs per unit**, so a price change never looks like a re-measurement or flips a
  decision; dollars are reported alongside. A skill version is published (and should be released) only if it uses at
  least `min_improvement_pct` fewer DBUs per unit than the cheapest published benchmark. A skill with no baseline is
  skipped: measure first. A skill version is compared only if its last run is newer than the last baseline run, so an old skill measurement is never released against a re-measured baseline.
  Current changes only when another published benchmark is at least `min_improvement_pct`
  cheaper, so run-to-run noise cannot flip it.

## Architecture

```
 Engineer (scripted run)                       Platform (system tables)
 ------------------------                      -------------------------
 01_log_benchmark_run  ──start/end──►  benchmark_runs (build log, engineering only)
                                                    │
                                                    ▼
 system.billing.usage (hourly, per user) ──►  v_benchmark_run_costs  ◄── system.access.assistant_events
                                                    │   per-run DBUs, turns, noise turns, overlaps
                                                    ▼
 job: 02_compute_and_publish ──────────►  benchmark_run_results (status per run)
                                                    │   aggregate engineer runs per workload/config/effort
                                                    ▼
                                         release gate ──► benchmark_decisions (audit trail)
                                                    │
                                                    ▼
                                         unit_cost_benchmarks (versioned; is_current = true)
                                                    │
                     ┌──────────────────────────────┼─────────────────────────────┐
                     ▼                              ▼                             ▼
          AI/BI dashboard                 Team budgets                 Forecast skill (future)
          (unit costs, $100 buys,         (unit cost x planned         reads is_current rows
           release gate, run log)          builds)
```

### Run statuses

| Status | Meaning |
|---|---|
| `valid` | Measured run with billing landed, no overlaps, no noise. Counted. |
| `pending_billing` | Less than `billing_lag_hours` since the end of the run's last billed hour. Re-run the job later. |
| `excluded_overlap` | Another run by the same user shares a clock hour; cost cannot be separated. |
| `excluded_noise` | The user sent Genie Code messages in the attributed hours outside the run window. |
| `no_billing` | No Genie Code billing found for the window. |
| `no_price` | Some billing rows could not be priced from the system tables (for example, free usage in a workspace that never billed the paid Genie Code SKU). Not counted. |
| `excluded_no_artifacts` | The run has `artifacts_produced` of 0 or empty (only possible by editing `benchmark_runs` directly; the notebook requires at least 1). Not counted, since there are no units to divide by. |

### Release-gate decisions

| Decision | When |
|---|---|
| `PUBLISH_BASELINE` | First measurement for a workload and effort level |
| `PUBLISH_IMPROVED` | Skill version uses at least `min_improvement_pct` fewer DBUs per unit than the cheapest published benchmark; becomes current (release the skill) |
| `REJECT_NO_IMPROVEMENT` | Skill version does not use at least `min_improvement_pct` fewer DBUs per unit (do not release) |
| `SKIP_NO_BASELINE` | Skill tested before any baseline exists |
| `SKIP_INSUFFICIENT_RUNS` | Fewer than `min_runs` valid engineer runs |
| `SKIP_HIGH_SPREAD` | The last `min_runs` runs disagree: (highest - lowest DBUs per unit) / the last run's DBUs per unit is above `max_spread_pct`. Log another run; the published numbers stay as they were until the last runs agree |
| `SKIP_STALE_SKILL` | The skill version's last run is not newer than the last baseline run (for example, the baseline was re-measured after the skill was tested). Re-run the skill |
| `REFRESHED` | A published benchmark (current or superseded, including the baseline) was re-measured; its numbers are updated |
| `CURRENT_CHANGED` | After a refresh, another published benchmark is at least `min_improvement_pct` cheaper (DBUs per unit) than the current one and becomes current (for example, a released skill re-measured well above the baseline) |

Business-user runs on the same config do not change the published engineer benchmark. They produce a
`business_user_multiplier` (last business-user run's DBUs per unit / last engineer run's DBUs per unit) to size tier budgets.

## Prerequisites

* Databricks CLI v0.281+ (bundle dashboards with `dataset_catalog`/`dataset_schema`); tested with v1.19.0
* A workspace with serverless jobs and a SQL warehouse
* `SELECT` on `system.billing.usage`, `system.billing.account_prices`, `system.billing.list_prices`, and
  `system.access.assistant_events` for whoever runs the job
* A catalog where you can create a schema (the job creates `genie_code_benchmarks` if missing)
* A CLI profile for the workspace (`databricks auth login --host <workspace-url> --profile <name>`)

## Deploy and run

```bash
# 0. Point at your workspace and supply the required values (nothing environment-specific is committed)
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_catalog=<catalog>
export BUNDLE_VAR_warehouse_id=<warehouse_id>

# 1. Validate
databricks bundle validate --strict -t dev

# 2. Deploy the job and dashboard
databricks bundle deploy -t dev

# 3. Create tables, seed the standard workloads, compute and publish
databricks bundle run unit_cost_benchmarks -t dev

```

### Running a real benchmark

1. Open `src/notebooks/01_log_benchmark_run`. Use the notebook, not a Genie Code conversation, to start and end runs:
   any Genie Code message in the run's hours counts toward its cost or marks it as noise.
2. `action = start`, choose `workload_id`, `config` (`baseline` or the skill name), `skill_version`, `effort`.
3. Paste the workload `script_prompt` into a **new** Genie Code conversation and finish the build.
4. `action = end`, enter `artifacts_produced`.
5. After billing lands (default 6 hours), run the job again. Check the **Release gate** table on the dashboard.

Rules: one run per person per clock hour, no other Genie Code use by the runner in those hours, same prompt
script for every run of a workload.

## Configuration

Bundle variables (`databricks.yml`), passed to the job as parameters. Set them per target in `databricks.yml`,
with `--var name=value`, or with `BUNDLE_VAR_<name>` environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `catalog` | required | Catalog for the benchmark schema |
| `schema` | `genie_code_benchmarks` | Schema for all benchmark objects |
| `warehouse_id` | required | Warehouse for the dashboard |
| `billing_lag_hours` | `6` | Hours after the end of a run's last billed hour before its billing is treated as final |
| `min_runs` | `2` | Valid engineer runs needed before a candidate is evaluated |
| `max_spread_pct` | `25` | If the last `min_runs` runs differ by more than this (% of the last run's DBUs per unit), log another run before publishing |
| `min_improvement_pct` | `10` | How many % fewer DBUs per unit a skill version must use to be published |

The `max_spread_pct` and `min_improvement_pct` defaults are starting points, not measured values. Tune them once
real runs show how much Genie Code cost varies; keep `min_improvement_pct` above the typical run-to-run spread so
noise cannot pass the release gate.

## File structure

```
databricks.yml                              Bundle config, variables, targets (dev + commented production example)
resources/
  unit_cost_benchmarks.job.yml              Job: setup -> compute_and_publish
  unit_cost_dashboard.dashboard.yml         AI/BI dashboard resource
src/notebooks/
  00_setup.py                               Schema, tables, attribution view, workload seed (idempotent)
  01_log_benchmark_run.py                   Engineer notebook to start/end/cancel a benchmark run
  02_compute_and_publish.py                 Attribution, statuses, candidate unit costs, release gate, publish
src/dashboards/
  unit_cost_benchmarks.lvdash.json          Dashboard: KPIs, baseline vs skill, $100 buys, release gate, run log
```

## Tables

| Table | Grain |
|---|---|
| `benchmark_workloads` | One row per standard workload (8 seeded; 3 active) |
| `benchmark_runs` | One row per scripted run (engineering build log) |
| `v_benchmark_run_costs` | View: per-run DBUs, turns, noise turns, overlaps |
| `benchmark_run_results` | Per-run snapshot with status (rewritten each job run) |
| `unit_cost_benchmarks` | One row per published benchmark version; `is_current` marks the active one |
| `benchmark_decisions` | Append-only audit trail. A row is added only when it differs from that candidate's latest row (any decision), so re-running the job with the same runs adds nothing |

## Limitations

* Attribution is per user-hour within one workspace. Runs must be isolated; the job flags but cannot fix contaminated hours.
* The dashboard is empty until the first runs clear `billing_lag_hours` and the job runs again.
* Benchmarks are ratios for scripted work; real builds vary with requirements clarity. Re-baseline after each
  skill release or Genie Code model change (log new baseline runs; the latest one becomes the baseline once the last
  `min_runs` runs agree), then re-run any skill you still want to test, and compare business-user runs to engineer runs.
* A published unit cost is one run's measurement. Keep `max_spread_pct` tight enough that a single outlier cannot pass
  the consistency check.

## Disclaimer

This accelerator is provided as-is as community content and is not an officially supported Databricks
product. Unit costs are estimates computed from system tables for scripted benchmark runs. Validate them
against your actual billing before relying on them for budgets.
