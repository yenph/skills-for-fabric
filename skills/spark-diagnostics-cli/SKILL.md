---
name: spark-diagnostics-cli
description: >
  Diagnose Microsoft Fabric Spark job failures, monitor Livy session health, and analyze
  performance bottlenecks using Fabric REST APIs and CLI tools.
  Use when the user wants to: (1) diagnose failed Spark applications, job instances, or notebook failures,
  (2) check Livy session health and recover stuck sessions, (3) identify performance
  bottlenecks like OOM, shuffle spill, or data skew, (4) analyze Spark application
  metrics and resource usage via the Spark Advisor and Resource Usage APIs,
  (5) retrieve and interpret driver and executor logs without an active session.
  Triggers: "spark diagnostics", "debug spark job", "failed spark notebook",
  "spark session health", "spark performance", "slow spark query", "spark out of memory",
  "spark job monitoring", "notebook failure diagnostics", "Livy session stuck",
  "spark shuffle spill", "spark data skew".
---

> **Update Check — ONCE PER SESSION (mandatory)**
> The first time this skill is used in a session, run the **check-updates** skill before proceeding.
> - **GitHub Copilot CLI / VS Code**: invoke the `check-updates` skill.
> - **Claude Code / Cowork / Cursor / Windsurf / Codex**: compare local vs remote package.json version.
> - Skip if the check was already performed earlier in this session.

> **CRITICAL NOTES**
> 1. To find the workspace details (including its ID) from workspace name: list all workspaces and, then, use JMESPath filtering
> 2. To find the item details (including its ID) from workspace ID, item type, and item name: list all items of that type in that workspace and, then, use JMESPath filtering
> 3. **Skill disambiguation**: `spark-diagnostics-cli` is for **read-only triage and diagnosis** of existing jobs and sessions. For creating notebooks, running new jobs, or Spark development, use `spark-authoring-cli`. For interactive PySpark analysis and Livy session creation, use `spark-consumption-cli`.

# Spark Diagnostics — CLI Skill

## Table of Contents

| Task | Reference | Notes |
|---|---|---|
| Fabric Topology & Key Concepts | [COMMON-CORE.md § Fabric Topology & Key Concepts](../../common/COMMON-CORE.md#fabric-topology--key-concepts) ||
| Environment URLs | [COMMON-CORE.md § Environment URLs](../../common/COMMON-CORE.md#environment-urls) ||
| Authentication & Token Acquisition | [COMMON-CORE.md § Authentication & Token Acquisition](../../common/COMMON-CORE.md#authentication--token-acquisition) | Wrong audience = 401; read before any auth issue |
| Core Control-Plane REST APIs | [COMMON-CORE.md § Core Control-Plane REST APIs](../../common/COMMON-CORE.md#core-control-plane-rest-apis) ||
| Pagination | [COMMON-CORE.md § Pagination](../../common/COMMON-CORE.md#pagination) ||
| Long-Running Operations (LRO) | [COMMON-CORE.md § Long-Running Operations (LRO)](../../common/COMMON-CORE.md#long-running-operations-lro) ||
| Rate Limiting & Throttling | [COMMON-CORE.md § Rate Limiting & Throttling](../../common/COMMON-CORE.md#rate-limiting--throttling) ||
| Job Execution | [COMMON-CORE.md § Job Execution](../../common/COMMON-CORE.md#job-execution) ||
| Capacity Management | [COMMON-CORE.md § Capacity Management](../../common/COMMON-CORE.md#capacity-management) ||
| Gotchas & Troubleshooting | [COMMON-CORE.md § Gotchas & Troubleshooting](../../common/COMMON-CORE.md#gotchas--troubleshooting) ||
| Best Practices | [COMMON-CORE.md § Best Practices](../../common/COMMON-CORE.md#best-practices) ||
| Tool Selection Rationale | [COMMON-CLI.md § Tool Selection Rationale](../../common/COMMON-CLI.md#tool-selection-rationale) ||
| Finding Workspaces and Items in Fabric | [COMMON-CLI.md § Finding Workspaces and Items in Fabric](../../common/COMMON-CLI.md#finding-workspaces-and-items-in-fabric) | **Mandatory** — *READ link first* [needed for finding workspace id by its name or item id by its name, item type, and workspace id] |
| Authentication Recipes | [COMMON-CLI.md § Authentication Recipes](../../common/COMMON-CLI.md#authentication-recipes) | `az login` flows and token acquisition |
| Fabric Control-Plane API via `az rest` | [COMMON-CLI.md § Fabric Control-Plane API via az rest](../../common/COMMON-CLI.md#fabric-control-plane-api-via-az-rest) | **Always pass `--resource https://api.fabric.microsoft.com`** or `az rest` fails |
| Pagination Pattern | [COMMON-CLI.md § Pagination Pattern](../../common/COMMON-CLI.md#pagination-pattern) ||
| Long-Running Operations (LRO) Pattern | [COMMON-CLI.md § Long-Running Operations (LRO) Pattern](../../common/COMMON-CLI.md#long-running-operations-lro-pattern) ||
| Gotchas & Troubleshooting (CLI-Specific) | [COMMON-CLI.md § Gotchas & Troubleshooting (CLI-Specific)](../../common/COMMON-CLI.md#gotchas--troubleshooting-cli-specific) | `az rest` audience, shell escaping, token expiry |
| Quick Reference: `az rest` Template | [COMMON-CLI.md § Quick Reference: az rest Template](../../common/COMMON-CLI.md#quick-reference-az-rest-template) ||
| Quick Reference: Token Audience / CLI Tool Matrix | [COMMON-CLI.md § Quick Reference: Token Audience ↔ CLI Tool Matrix](../../common/COMMON-CLI.md#quick-reference-token-audience--cli-tool-matrix) | Which `--resource` + tool for each service |
| Livy Session Management | [SPARK-CONSUMPTION-CORE.md § Livy Session Management](../../common/SPARK-CONSUMPTION-CORE.md#livy-session-management) | Session creation, states, lifecycle, termination |
| Interactive Data Exploration | [SPARK-CONSUMPTION-CORE.md § Interactive Data Exploration](../../common/SPARK-CONSUMPTION-CORE.md#interactive-data-exploration) | Statement execution, output retrieval, data discovery |
| Notebook Execution & Job Management | [SPARK-AUTHORING-CORE.md § Notebook Execution & Job Management](../../common/SPARK-AUTHORING-CORE.md#notebook-execution--job-management) ||
| Job Failure Classification | [job-diagnostics.md § Failure Classification](references/job-diagnostics.md#failure-classification) | OOM, shuffle, timeout, dependency, configuration errors |
| Reading Spark Logs via REST | [job-diagnostics.md § Reading Spark Logs via REST](references/job-diagnostics.md#reading-spark-logs-via-rest) | Driver/executor log retrieval from Livy |
| Job Instance History | [job-diagnostics.md § Job Instance History](references/job-diagnostics.md#job-instance-history) | Query recent runs, compare durations, detect regressions |
| Failure Triage Workflow | [job-diagnostics.md § Failure Triage Workflow](references/job-diagnostics.md#failure-triage-workflow) | Step-by-step decision tree for diagnosing failures |
| Session Health Assessment | [session-health.md § Livy Session Lifecycle](references/session-health.md#livy-session-lifecycle) | Session states, transitions, expected durations |
| Idle and Zombie Session Detection | [session-health.md § Idle and Zombie Session Detection](references/session-health.md#idle-and-zombie-session-detection) | Find and clean up leaked sessions |
| Session Resource Monitoring | [session-health.md § Session Resource Monitoring](references/session-health.md#session-resource-monitoring) | Memory and executor usage via Livy |
| Session Recovery Patterns | [session-health.md § Session Recovery Patterns](references/session-health.md#session-recovery-patterns) | Restart strategies and session replacement |
| Performance Anti-Patterns | [performance-patterns.md § Anti-Patterns](references/performance-patterns.md#anti-patterns) | Spill, shuffle, skew, small files, collect misuse |
| Stage and Task Analysis | [performance-patterns.md § Stage and Task Analysis](references/performance-patterns.md#stage-and-task-analysis) | Reading Spark UI metrics via REST |
| Optimization Recipes | [performance-patterns.md § Optimization Recipes](references/performance-patterns.md#optimization-recipes) | Partition tuning, broadcast joins, caching |
| Capacity and Resource Diagnostics | [performance-patterns.md § Capacity and Resource Diagnostics](references/performance-patterns.md#capacity-and-resource-diagnostics) | CU consumption, throttling detection |
| Spark Monitoring API Overview | [SPARK-MONITORING-CORE.md § Overview](../../common/SPARK-MONITORING-CORE.md#overview) | GA monitoring APIs — no active session required |
| Workspace & Item Session Listing | [SPARK-MONITORING-CORE.md § Workspace and Item-Level Session Listing](../../common/SPARK-MONITORING-CORE.md#workspace-and-item-level-session-listing) | List Spark apps across workspace with filtering |
| Open-Source Spark History Server APIs | [SPARK-MONITORING-CORE.md § Open-Source Spark History Server APIs](../../common/SPARK-MONITORING-CORE.md#open-source-spark-history-server-apis) | Jobs, stages, executors, SQL queries via REST |
| Driver and Executor Log APIs | [SPARK-MONITORING-CORE.md § Driver and Executor Log APIs](../../common/SPARK-MONITORING-CORE.md#driver-and-executor-log-apis) | Direct log retrieval without active session |
| Livy Log API | [SPARK-MONITORING-CORE.md § Livy Log API](../../common/SPARK-MONITORING-CORE.md#livy-log-api) | Session-level log with byte-offset pagination |
| Spark Advisor API | [SPARK-MONITORING-CORE.md § Spark Advisor API](../../common/SPARK-MONITORING-CORE.md#spark-advisor-api) | **Key** — automated skew detection, task errors, recommendations |
| Resource Usage API | [SPARK-MONITORING-CORE.md § Resource Usage API](../../common/SPARK-MONITORING-CORE.md#resource-usage-api) | vCore timeline, idle/running cores, efficiency metrics |
| Monitoring Diagnostic Workflow | [SPARK-MONITORING-CORE.md § Diagnostic Workflow Using Monitoring APIs](../../common/SPARK-MONITORING-CORE.md#diagnostic-workflow-using-monitoring-apis) | Step-by-step triage using monitoring APIs |

---

## Must/Prefer/Avoid

### MUST DO

- Always retrieve job/session status before attempting remediation
- Use workspace and item discovery from [COMMON-CLI.md](../../common/COMMON-CLI.md#finding-workspaces-and-items-in-fabric) — never hardcode IDs
- Check Livy session state before submitting diagnostic statements
- Follow the [Failure Triage Workflow](references/job-diagnostics.md#failure-triage-workflow) for systematic diagnosis
- Always check the Spark Advisor API before reading raw logs — it often identifies the root cause immediately
- Use monitoring APIs (no active session required) before attempting Livy-based diagnostics
- Poll job/session status with 10–30 second intervals; timeout diagnostics after 30 minutes

### PREFER

- Querying job instance history to establish baseline before declaring a regression
- Reusing existing idle sessions for diagnostic queries instead of creating new ones
- Checking capacity utilization when jobs are slow before blaming the Spark code
- Using `az rest` with JMESPath filtering to extract specific fields from large API responses
- The Spark Advisor API over manual log parsing for skew, task errors, and timeout detection
- Resource Usage API `coreEfficiency` metric to quantify cluster utilization before recommending scaling
- Job instance history comparison (last 5 runs) to detect regressions before deep-diving

### AVOID

- Killing sessions without checking if they have active statements
- Creating new sessions for every diagnostic query (reuse idle sessions)
- Assuming OOM without checking actual memory metrics from Livy
- Hardcoded workspace or item IDs in diagnostic scripts
- Diagnosing performance without first checking capacity throttling via the Admin API
- Submitting diagnostic statements to sessions in `busy` state

---

## Quick Start

### Environment Setup

Apply environment detection from [COMMON-CLI.md](../../common/COMMON-CLI.md#authentication-recipes) to set:
- `$FABRIC_API_BASE` and `$FABRIC_RESOURCE_SCOPE`
- `$FABRIC_API_URL` and `$LIVY_API_PATH` for Livy operations

**Authentication**: Use token acquisition from [COMMON-CLI.md § Authentication Recipes](../../common/COMMON-CLI.md#authentication-recipes).

### Diagnose a Failed Notebook Run

```bash
# 1. Discover workspace and notebook
workspaceId=$(az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces" \
  --query "value[?displayName=='MyWorkspace'].id" --output tsv)

notebookId=$(az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items?type=Notebook" \
  --query "value[?displayName=='MyNotebook'].id" --output tsv)

# 2. Get recent job instances
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items/$notebookId/jobs/instances?limit=5" \
  --query "value[].{id:id, status:status, start:startTimeUtc, end:endTimeUtc, failureReason:failureReason}" \
  --output table

# 3. Get details of the failed instance
jobInstanceId="<from-above>"
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items/$notebookId/jobs/instances/$jobInstanceId" \
  --output json
```

### Check Livy Session Health

```bash
# List all sessions for a lakehouse
lakehouseId="<lakehouse-id>"
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/lakehouses/$lakehouseId/$LIVY_API_PATH/sessions" \
  --query "sessions[].{id:id, state:state, name:name, appId:appId}" \
  --output table

# Get detailed session info (includes memory, executors)
sessionId="<session-id>"
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/lakehouses/$lakehouseId/$LIVY_API_PATH/sessions/$sessionId" \
  --output json
```

### Quick Performance Check via Livy Statement

```bash
# Run a diagnostic PySpark snippet in an existing idle session
cat > /tmp/body.json << 'DIAG'
{
  "code": "sc = spark.sparkContext\nprint('Active executors:', len(sc._jsc.sc().getExecutorMemoryStatus()))\nprint('Default parallelism:', sc.defaultParallelism)\nprint('Spark config:')\nfor k, v in sorted(spark.sparkContext.getConf().getAll()):\n    if any(x in k for x in ['memory', 'cores', 'parallelism', 'shuffle', 'dynamic']):\n        print(f'  {k} = {v}')",
  "kind": "pyspark"
}
DIAG
az rest --method post --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/lakehouses/$lakehouseId/$LIVY_API_PATH/sessions/$sessionId/statements" \
  --body @/tmp/body.json --output json
```

## Key Diagnostic Patterns

| Symptom | First Check | Likely Cause | Reference |
|---|---|---|---|
| Job failed with error | Job instance `failureReason` | See failure classification | [job-diagnostics.md](references/job-diagnostics.md#failure-classification) |
| Session stuck in `starting` | Session state + elapsed time | Capacity pressure or pool misconfiguration | [session-health.md](references/session-health.md#livy-session-lifecycle) |
| Job runs but is very slow | Stage metrics + executor count | Shuffle spill, data skew, under-provisioning | [performance-patterns.md](references/performance-patterns.md#anti-patterns) |
| `OutOfMemoryError` in logs | Driver vs executor OOM | Wrong memory config or data explosion | [job-diagnostics.md](references/job-diagnostics.md#failure-classification) |
| Many idle sessions | Session list with state filter | Session leak — clean up | [session-health.md](references/session-health.md#idle-and-zombie-session-detection) |
| Job slower than yesterday | Job history duration comparison | Regression or data volume growth | [job-diagnostics.md](references/job-diagnostics.md#job-instance-history) |

---

**Focus**: This skill provides Fabric-specific diagnostic patterns using REST APIs. LLM already knows Spark internals — we focus on Fabric API access, session management, and systematic triage workflows.
