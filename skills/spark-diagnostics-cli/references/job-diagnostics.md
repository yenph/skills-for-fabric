# Job Diagnostics

> **Scope**: Classify Spark job failures, retrieve logs via Fabric REST APIs, analyze job instance history, and follow a systematic triage workflow. All examples use `az rest` against the Fabric API.

---

## Failure Classification

Spark job failures in Fabric fall into distinct categories. Identify the category first — it determines the remediation path.

### Out of Memory (OOM)

**Symptoms**:
- `java.lang.OutOfMemoryError: Java heap space` in driver or executor
- `java.lang.OutOfMemoryError: GC overhead limit exceeded`
- Container killed by YARN: `Container killed by the ApplicationMaster. ... Exit code is 137`
- `ExecutorLostFailure (executor N exited caused by one of the running tasks)`

**Common Causes**:
| Cause | Indicator | Fix |
|---|---|---|
| Driver OOM from `collect()` | Error in driver logs after `.collect()` call | Replace with `.limit(N).toPandas()` or write to table |
| Driver OOM from `toPandas()` | Large DataFrame converted to Pandas | Use `.limit()` or process in Spark |
| Executor OOM from wide transforms | Error during `shuffle`, `join`, or `groupBy` | Increase executor memory or repartition input |
| Broadcast join on large table | OOM during broadcast | Set `spark.sql.autoBroadcastJoinThreshold=-1` or explicitly repartition |
| Skewed partition | Single executor OOM while others are fine | Enable AQE skew join: `spark.sql.adaptive.skewJoin.enabled=true` |

**Diagnostic Query** (run in Livy session):
```python
# Check for skewed partitions (large variance = skew)
df = spark.table("your_table")
partition_sizes = df.groupBy(spark.sparkContext.partitionId().alias("pid")).count()
partition_sizes.describe("count").show()
# If max >> mean, you have data skew
```

### Shuffle Failures

**Symptoms**:
- `FetchFailedException: Failed to connect to host`
- `ShuffleMapTask failed with FetchFailed`
- `Too large frame` errors
- `Connection reset by peer` during shuffle read

**Common Causes**:
| Cause | Indicator | Fix |
|---|---|---|
| Executor lost during shuffle | `FetchFailedException` after executor OOM | Increase memory or reduce shuffle partition count |
| Network timeout | `Connection timed out` in shuffle fetch | Increase `spark.network.timeout` (default 120s) |
| Excessive shuffle data | Shuffle write > available disk | Reduce data before shuffle, add pre-filters |
| Too many shuffle partitions | Thousands of small tasks | Set `spark.sql.shuffle.partitions` to 2-4x core count |

### Timeout Failures

**Symptoms**:
- `Job cancelled because SparkContext was shut down`
- `Task not serializable`
- Session transitions to `dead` state after long idle
- `Livy session has expired`

**Common Causes**:
| Cause | Indicator | Fix |
|---|---|---|
| Livy session timeout | Session `dead` after inactivity | Increase `spark.livy.server.session.timeout` or keep-alive |
| Long-running task | Single task runs for hours | Check for data skew or Cartesian join |
| Spark context shutdown | Context killed by Fabric | Check capacity throttling; retry with smaller dataset |

### Dependency and Configuration Errors

**Symptoms**:
- `ClassNotFoundException` or `NoSuchMethodError`
- `Py4JJavaError: An error occurred while calling o123.load`
- `AnalysisException: Table or view not found`
- `IllegalArgumentException` from invalid config values

**Common Causes**:
| Cause | Indicator | Fix |
|---|---|---|
| Missing library | `ClassNotFoundException` with library class name | Install library via Fabric Environment |
| Wrong library version | `NoSuchMethodError` | Check library version compatibility |
| Table not found | `AnalysisException` | Verify lakehouse attachment and table name |
| Invalid Spark config | `IllegalArgumentException` | Validate config key and value format |

---

## Reading Spark Logs via REST

> **Tip**: For completed or failed Spark applications, the [Driver and Executor Log APIs](../../../common/SPARK-MONITORING-CORE.md#driver-and-executor-log-apis) provide direct REST access to logs without requiring an active Livy session. The [Livy Log API](../../../common/SPARK-MONITORING-CORE.md#livy-log-api) offers byte-offset pagination for large logs.

### Retrieve Livy Session Logs

Livy exposes driver logs through the session API. This is the primary way to get Spark logs in Fabric without accessing the cluster directly.

```bash
# Get the last 100 lines of driver output
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/lakehouses/$lakehouseId/$LIVY_API_PATH/sessions/$sessionId/log?from=0&size=100" \
  --query "log" --output tsv
```

**Pagination for large logs**:
```bash
# Get total log lines first
total=$(az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/lakehouses/$lakehouseId/$LIVY_API_PATH/sessions/$sessionId/log?from=0&size=1" \
  --query "total" --output tsv)

# Get the last 200 lines (where errors usually are)
from=$((total - 200))
[ $from -lt 0 ] && from=0
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/lakehouses/$lakehouseId/$LIVY_API_PATH/sessions/$sessionId/log?from=$from&size=200" \
  --query "log" --output tsv
```

### Retrieve Statement Output for Errors

When a Livy statement fails, the error is in the statement output:

```bash
# Get statement result (includes traceback for failed statements)
statementId="<statement-id>"
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/lakehouses/$lakehouseId/$LIVY_API_PATH/sessions/$sessionId/statements/$statementId" \
  --query "{state:state, output:output}" --output json
```

The `output` object contains:
- `status`: `"ok"` or `"error"`
- `evalue`: Error message text
- `traceback`: Full Python/Java traceback as array of strings

---

## Job Instance History

### Query Recent Job Runs

Use job instance APIs to compare runs over time and detect regressions.

```bash
# Get last 10 job instances for a notebook
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items/$notebookId/jobs/instances?limit=10" \
  --query "value[].{id:id, status:status, start:startTimeUtc, end:endTimeUtc, failureReason:failureReason}" \
  --output table
```

### Compare Job Durations

```bash
# Extract durations for trend analysis
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items/$notebookId/jobs/instances?limit=20" \
  --query "value[?status=='Completed'].{start:startTimeUtc, end:endTimeUtc}" \
  --output json
```

To compute duration differences, pipe through `jq` or process in a Livy session:

```bash
# Using jq to compute durations (if available)
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items/$notebookId/jobs/instances?limit=20" \
  --output json | jq '.value[] | select(.status=="Completed") |
    {start: .startTimeUtc, end: .endTimeUtc, status: .status}'
```

### Detect Regressions

A job is regressing if the latest successful run is significantly slower than the median of the previous runs. Compare the last run's duration against the median of the 5 runs before it. If the ratio exceeds 2x, investigate data volume changes first, then Spark configuration drift.

---

## Failure Triage Workflow

Follow this decision tree when a Spark job fails in Fabric.

### Step 1: Get the Job Status

```bash
# What is the job's current state?
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items/$notebookId/jobs/instances/$jobInstanceId" \
  --query "{status:status, failureReason:failureReason}" --output json
```

| Status | Next Step |
|---|---|
| `Failed` | Go to Step 2 — read the failure reason |
| `Cancelled` | Check if user-cancelled or timeout-killed (Step 3) |
| `InProgress` | Job is still running — check elapsed time vs historical average |
| `Completed` | Job succeeded — if performance concern, go to [performance-patterns.md](performance-patterns.md) |
| `Deduped` | Another instance was already running — check that instance instead |

### Step 2: Classify the Failure

Read `failureReason` from the job instance response. Match against the categories in [Failure Classification](#failure-classification):

1. **Contains `OutOfMemoryError`** → OOM category
2. **Contains `FetchFailedException` or `ShuffleMapTask`** → Shuffle failure
3. **Contains `timeout` or `expired`** → Timeout category
4. **Contains `ClassNotFoundException` or `AnalysisException`** → Dependency/config error
5. **None of the above** → Read the full Livy session log (Step 4)

### Step 3: Check for Cancellation Cause

```bash
# Was the job cancelled by the user or by the system?
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/items/$notebookId/jobs/instances/$jobInstanceId" \
  --query "{status:status, invokeType:invokeType, failureReason:failureReason}" --output json
```

- `invokeType: "Manual"` + cancelled → user cancelled
- System cancellation → check capacity throttling or session timeout

### Step 4: Read the Logs

If `failureReason` is not descriptive enough, retrieve the Livy session logs using the patterns in [Reading Spark Logs via REST](#reading-spark-logs-via-rest). Search for:
- `ERROR` or `FATAL` log lines
- Java exception stack traces (lines starting with `at `)
- Python tracebacks (lines starting with `Traceback` or `File "`)

> **Alternative**: Use the [Driver Log API](../../../common/SPARK-MONITORING-CORE.md#driver-and-executor-log-apis) to access driver stderr directly, or the [Executor Log API](../../../common/SPARK-MONITORING-CORE.md#driver-and-executor-log-apis) for per-executor logs. These APIs work on completed applications without an active session.

### Step 4b: Check Spark Advisor

Before manual analysis, check if the Spark Advisor has already identified the issue:

```bash
az rest --method get --resource "$FABRIC_RESOURCE_SCOPE" \
  --url "$FABRIC_API_URL/workspaces/$workspaceId/notebooks/$notebookId/livySessions/$livyId/applications/$appId/advice" \
  --output json
```

The Advisor automatically detects data skew, time skew, and task errors. See [Spark Advisor API](../../../common/SPARK-MONITORING-CORE.md#spark-advisor-api).

### Step 5: Apply the Fix

Once classified, apply the remediation from the corresponding category in [Failure Classification](#failure-classification). After applying, re-run the job and monitor using the [Job Instance History](#job-instance-history) patterns to confirm the fix.
