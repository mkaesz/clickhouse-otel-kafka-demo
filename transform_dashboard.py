#!/usr/bin/env python3
"""
Transform 23285_rev1.json (Prometheus/PromQL) → ClickHouse OTel SQL.
Generates grafana/dashboards/ch-customer-{1,2}-v2.json.
"""

import json, re, copy

CH_DS = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse-poc"}
REFIDS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# ---------------------------------------------------------------------------
# Metric classification
# ---------------------------------------------------------------------------

def mtype(name):
    if name.startswith(("ClickHouseMetrics_", "ClickHouseAsyncMetrics_")):
        return "gauge"
    if name.startswith(("ClickHouseProfileEvents_", "ClickHouseErrorMetric_")):
        return "sum"
    return None

def otable(name):
    t = mtype(name)
    return f"otel.otel_metrics_{t}" if t else None

def is_node(expr):
    return bool(re.search(r"\bnode_\w+\b|instance:node_cpu:ratio", expr))

def extract_metrics(expr):
    return re.findall(
        r"ClickHouse(?:Metrics|AsyncMetrics|ProfileEvents|ErrorMetric)_\w+", expr
    )

def safe_col(metric):
    parts = metric.split("_", 2)
    tail = parts[-1] if len(parts) >= 3 else metric
    return re.sub(r"[^a-z0-9]", "_", tail.lower())[:48]

# ---------------------------------------------------------------------------
# Generic SQL target builders
# ---------------------------------------------------------------------------

def ts_gauge(cluster, metric, ref="A", label=None):
    col = label or safe_col(metric)
    sql = (
        f"SELECT toStartOfMinute(TimeUnix) AS time, avg(Value) AS {col} "
        f"FROM otel.otel_metrics_gauge "
        f"WHERE ServiceName = '{cluster}' AND MetricName = '{metric}' "
        f"AND TimeUnix >= $__fromTime AND TimeUnix <= $__toTime "
        f"GROUP BY time ORDER BY time"
    )
    return {"refId": ref, "queryType": "timeseries", "datasource": CH_DS, "rawSql": sql}

def ts_sum(cluster, metric, ref="A", label=None):
    col = label or safe_col(metric)
    sql = (
        f"SELECT toStartOfMinute(TimeUnix) AS time, "
        f"greatest(max(Value) - min(Value), 0) AS {col} "
        f"FROM otel.otel_metrics_sum "
        f"WHERE ServiceName = '{cluster}' AND MetricName = '{metric}' "
        f"AND TimeUnix >= $__fromTime AND TimeUnix <= $__toTime "
        f"GROUP BY time ORDER BY time"
    )
    return {"refId": ref, "queryType": "timeseries", "datasource": CH_DS, "rawSql": sql}

def ts_any(cluster, metric, ref="A", label=None):
    """Dispatch to gauge or sum builder based on metric prefix."""
    t = mtype(metric)
    if t == "gauge":
        return ts_gauge(cluster, metric, ref, label)
    if t == "sum":
        return ts_sum(cluster, metric, ref, label)
    return None

def ts_ratio(cluster, num, den, ref="A", label=None):
    n_tbl = otable(num)
    d_tbl = otable(den)
    lbl = label or f"{safe_col(num)}_per_{safe_col(den)}"
    sql = (
        f"SELECT t1.time AS time, t1.d / nullIf(t2.d, 0) AS {lbl} "
        f"FROM ("
        f"SELECT toStartOfMinute(TimeUnix) AS time, greatest(max(Value)-min(Value),0) AS d "
        f"FROM {n_tbl} WHERE ServiceName='{cluster}' AND MetricName='{num}' "
        f"AND TimeUnix>=$__fromTime AND TimeUnix<=$__toTime GROUP BY time) t1 "
        f"JOIN ("
        f"SELECT toStartOfMinute(TimeUnix) AS time, greatest(max(Value)-min(Value),0) AS d "
        f"FROM {d_tbl} WHERE ServiceName='{cluster}' AND MetricName='{den}' "
        f"AND TimeUnix>=$__fromTime AND TimeUnix<=$__toTime GROUP BY time) t2 "
        f"ON t1.time=t2.time ORDER BY t1.time"
    )
    return {"refId": ref, "queryType": "timeseries", "datasource": CH_DS, "rawSql": sql}

def tbl_stat(cluster, metric, ref="A", label=None):
    col = label or safe_col(metric)
    tbl = otable(metric)
    fn = "argMax" if mtype(metric) == "gauge" else "max"
    if mtype(metric) == "gauge":
        sql = (
            f"SELECT round(argMax(Value, TimeUnix), 6) AS {col} "
            f"FROM {tbl} WHERE ServiceName='{cluster}' AND MetricName='{metric}' "
            f"AND TimeUnix >= now() - INTERVAL 5 MINUTE"
        )
    else:
        sql = (
            f"SELECT greatest(max(Value)-min(Value),0) AS {col} "
            f"FROM {tbl} WHERE ServiceName='{cluster}' AND MetricName='{metric}' "
            f"AND TimeUnix >= now() - INTERVAL 5 MINUTE"
        )
    return {"refId": ref, "format": "table", "datasource": CH_DS, "rawSql": sql}

# ---------------------------------------------------------------------------
# Special panel SQL
# ---------------------------------------------------------------------------

def targets_14(cluster):
    sql = (
        f"SELECT "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseMetrics_VersionInteger')) AS version, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseMetrics_Revision')) AS revision, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_NumberOfDatabases')) AS databases, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_NumberOfTables')) AS tables, "
        f"round(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_Uptime'),0) AS uptime_s, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseMetrics_ReadonlyReplica')) AS readonly_replicas, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseMetrics_KafkaConsumers')) AS kafka_consumers, "
        f"round((1-argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_OSIdleTimeNormalized'))*100,1) AS cpu_pct, "
        f"round(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_OSUptime'),0) AS os_uptime_s "
        f"FROM otel.otel_metrics_gauge "
        f"WHERE ServiceName='{cluster}' "
        f"AND MetricName IN ('ClickHouseMetrics_VersionInteger','ClickHouseMetrics_Revision',"
        f"'ClickHouseAsyncMetrics_NumberOfDatabases','ClickHouseAsyncMetrics_NumberOfTables',"
        f"'ClickHouseAsyncMetrics_Uptime','ClickHouseMetrics_ReadonlyReplica',"
        f"'ClickHouseMetrics_KafkaConsumers','ClickHouseAsyncMetrics_OSIdleTimeNormalized',"
        f"'ClickHouseAsyncMetrics_OSUptime') "
        f"AND TimeUnix >= now() - INTERVAL 5 MINUTE"
    )
    return [{"refId": "A", "format": "table", "datasource": CH_DS, "rawSql": sql}]

def targets_145(cluster):
    sql = (
        f"SELECT "
        f"replaceRegexpOne(MetricName,'ClickHouseAsyncMetrics_Disk[A-Za-z]+_(.*)','\\\\1') AS disk, "
        f"maxIf(Value,MetricName LIKE '%DiskTotal_%') AS total, "
        f"maxIf(Value,MetricName LIKE '%DiskUsed_%') AS used, "
        f"maxIf(Value,MetricName LIKE '%DiskAvailable_%') AS available, "
        f"maxIf(Value,MetricName LIKE '%DiskUnreserved_%') AS unreserved, "
        f"round(maxIf(Value,MetricName LIKE '%DiskUsed_%') "
        f"/ nullIf(maxIf(Value,MetricName LIKE '%DiskTotal_%'),0)*100,1) AS usage_pct "
        f"FROM otel.otel_metrics_gauge "
        f"WHERE ServiceName='{cluster}' "
        f"AND (MetricName LIKE 'ClickHouseAsyncMetrics_DiskTotal_%' "
        f"OR MetricName LIKE 'ClickHouseAsyncMetrics_DiskUsed_%' "
        f"OR MetricName LIKE 'ClickHouseAsyncMetrics_DiskAvailable_%' "
        f"OR MetricName LIKE 'ClickHouseAsyncMetrics_DiskUnreserved_%') "
        f"AND TimeUnix >= now() - INTERVAL 5 MINUTE "
        f"GROUP BY disk ORDER BY disk"
    )
    return [{"refId": "A", "format": "table", "datasource": CH_DS, "rawSql": sql}]

def targets_disk_ts(cluster, like_pattern, col_label):
    sql = (
        f"SELECT toStartOfMinute(TimeUnix) AS time, "
        f"replaceRegexpOne(MetricName,'.*_([^_]+)$','\\\\1') AS disk, "
        f"avg(Value) AS {col_label} "
        f"FROM otel.otel_metrics_gauge "
        f"WHERE ServiceName='{cluster}' AND MetricName LIKE '{like_pattern}' "
        f"AND TimeUnix>=$__fromTime AND TimeUnix<=$__toTime "
        f"GROUP BY time, disk ORDER BY time"
    )
    return [{"refId": "A", "queryType": "timeseries", "datasource": CH_DS, "rawSql": sql}]

def targets_disk_delta(cluster, extra_min, delta_label):
    sql = (
        f"SELECT time, disk, {delta_label} FROM ("
        f"SELECT toStartOfMinute(TimeUnix) AS time, "
        f"replaceRegexpOne(MetricName,'.*_([^_]+)$','\\\\1') AS disk, "
        f"Value - lagInFrame(Value,1,Value) OVER (PARTITION BY MetricName ORDER BY TimeUnix) AS {delta_label} "
        f"FROM otel.otel_metrics_gauge "
        f"WHERE ServiceName='{cluster}' AND MetricName LIKE 'ClickHouseAsyncMetrics_DiskUsed_%' "
        f"AND TimeUnix >= $__fromTime - INTERVAL {extra_min} MINUTE AND TimeUnix <= $__toTime"
        f") ORDER BY time"
    )
    return [{"refId": "A", "queryType": "timeseries", "datasource": CH_DS, "rawSql": sql}]

def targets_227(cluster):
    sql = (
        f"SELECT "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_KeeperIsLeader')) AS is_leader, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_KeeperIsFollower')) AS is_follower, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_KeeperIsObserver')) AS is_observer, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_KeeperZnodeCount')) AS znode_count, "
        f"toUInt32(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_NumberOfTables')) AS tables, "
        f"round(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_Uptime'),0) AS uptime_s, "
        f"round((1-argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_OSIdleTimeNormalized'))*100,1) AS cpu_pct, "
        f"round(argMaxIf(Value,TimeUnix,MetricName='ClickHouseAsyncMetrics_OSUptime'),0) AS os_uptime_s "
        f"FROM otel.otel_metrics_gauge "
        f"WHERE ServiceName='{cluster}' "
        f"AND MetricName IN ('ClickHouseAsyncMetrics_KeeperIsLeader','ClickHouseAsyncMetrics_KeeperIsFollower',"
        f"'ClickHouseAsyncMetrics_KeeperIsObserver','ClickHouseAsyncMetrics_KeeperZnodeCount',"
        f"'ClickHouseAsyncMetrics_NumberOfTables','ClickHouseAsyncMetrics_Uptime',"
        f"'ClickHouseAsyncMetrics_OSIdleTimeNormalized','ClickHouseAsyncMetrics_OSUptime') "
        f"AND TimeUnix >= now() - INTERVAL 5 MINUTE"
    )
    return [{"refId": "A", "format": "table", "datasource": CH_DS, "rawSql": sql}]

# ---------------------------------------------------------------------------
# Panel IDs that need specific handling
# ---------------------------------------------------------------------------

NOT_MAPPABLE = {140, 137, 149, 148, 141, 142, 242}

# All panels nested inside Row 209 (Keeper Service Overview) — metrics come
# from the Keeper's own Prometheus endpoint, not from the ClickHouse server.
KEEPER_PANEL_IDS = {
    227, 210, 226, 215, 224, 225, 213, 214, 211, 212,
    217, 221, 216, 222, 219, 223, 218, 220,
}

# (numerator_metric, denominator_metric, label)
RATIO_PANELS = {
    8:   ("ClickHouseProfileEvents_QueryTimeMicroseconds",                "ClickHouseProfileEvents_Query",                "avg_query_us"),
    9:   ("ClickHouseProfileEvents_SelectQueryTimeMicroseconds",          "ClickHouseProfileEvents_SelectQuery",          "avg_select_us"),
    10:  ("ClickHouseProfileEvents_InsertQueryTimeMicroseconds",          "ClickHouseProfileEvents_InsertQuery",          "avg_insert_us"),
    69:  ("ClickHouseProfileEvents_DelayedInsertsMilliseconds",           "ClickHouseProfileEvents_DelayedInserts",       "avg_delay_ms"),
    80:  ("ClickHouseProfileEvents_MergeTotalMilliseconds",               "ClickHouseProfileEvents_Merge",                "avg_merge_ms"),
    108: ("ClickHouseProfileEvents_DistributedDelayedInsertsMilliseconds","ClickHouseProfileEvents_DistributedDelayedInserts","avg_dist_delay_ms"),
}

# ---------------------------------------------------------------------------
# Panel conversion
# ---------------------------------------------------------------------------

def base_panel(orig):
    p = {
        "id":        orig["id"],
        "title":     orig.get("title", ""),
        "type":      orig.get("type", "timeseries"),
        "gridPos":   orig.get("gridPos", {}),
        "datasource": CH_DS,
        "fieldConfig": orig.get("fieldConfig", {}),
        "options":   orig.get("options", {}),
    }
    for k in ("description", "transparent", "maxDataPoints"):
        if k in orig:
            p[k] = orig[k]
    return p

def not_mappable_text(orig):
    node_ms = set()
    for t in orig.get("targets", []):
        m = re.search(r"node_\w+|instance:node_cpu:ratio", t.get("expr", ""))
        if m:
            node_ms.add(m.group(0))
    return {
        "id": orig["id"],
        "gridPos": orig.get("gridPos", {}),
        "title": orig.get("title", ""),
        "type": "text",
        "datasource": CH_DS,
        "options": {
            "mode": "markdown",
            "content": (
                f"**Not available — requires Node Exporter**\n\n"
                f"Metric(s): `{', '.join(sorted(node_ms))}`\n\n"
                f"These metrics come from the Prometheus Node Exporter, not from the ClickHouse "
                f"scraper. They are not present in the OTel pipeline and cannot be shown here."
            ),
        },
    }

def convert_panel(orig, cluster, keeper_cluster):
    pid  = orig.get("id")
    ptype = orig.get("type", "")

    if ptype == "row":
        p = copy.deepcopy(orig)
        # Panels inside Row 209 belong to the Keeper service, not the CH cluster
        effective = keeper_cluster if orig.get("id") == 209 else cluster
        p["panels"] = [convert_panel(c, effective, keeper_cluster) for c in orig.get("panels", [])]
        return p

    if pid in NOT_MAPPABLE:
        return not_mappable_text(orig)

    p = base_panel(orig)

    # ---------- special panels ----------
    if pid == 14:
        p["targets"] = targets_14(cluster)
        p["transformations"] = []
        return p
    if pid == 145:
        p["targets"] = targets_145(cluster)
        p["transformations"] = []
        return p
    if pid == 199:
        p["targets"] = targets_disk_ts(cluster, "ClickHouseAsyncMetrics_DiskUsed_%", "used_bytes")
        return p
    if pid == 200:
        p["targets"] = targets_disk_delta(cluster, 12, "increase_10min")
        return p
    if pid == 201:
        p["targets"] = targets_disk_delta(cluster, 1445, "increase_daily")
        return p
    if pid == 244:
        p["targets"] = targets_disk_ts(cluster, "ClickHouseAsyncMetrics_DiskAvailable_%", "available_bytes")
        return p
    if pid == 245:
        p["targets"] = targets_disk_ts(cluster, "ClickHouseAsyncMetrics_DiskUnreserved_%", "unreserved_bytes")
        return p
    if pid == 227:
        p["targets"] = targets_227(cluster)
        p["transformations"] = []
        return p
    if pid in RATIO_PANELS:
        num, den, lbl = RATIO_PANELS[pid]
        p["targets"] = [ts_ratio(cluster, num, den, "A", lbl)]
        return p

    # ---------- generic conversion ----------
    new_targets = []
    ri = 0
    for t in orig.get("targets", []):
        if t.get("hide", False):
            continue
        expr = t.get("expr", "")
        if is_node(expr):
            continue
        metrics = extract_metrics(expr)
        if not metrics:
            continue
        metric = metrics[0]
        if not otable(metric):
            continue
        ref = REFIDS[ri] if ri < len(REFIDS) else f"T{ri}"
        tgt = ts_any(cluster, metric, ref)
        if tgt:
            new_targets.append(tgt)
            ri += 1

    if new_targets:
        p["targets"] = new_targets
    else:
        # Fallback: empty text panel so the grid position is preserved
        return {
            "id": pid,
            "gridPos": orig.get("gridPos", {}),
            "title": orig.get("title", ""),
            "type": "text",
            "datasource": CH_DS,
            "options": {"mode": "markdown", "content": "No OTel data source available for this panel."},
        }
    return p

# ---------------------------------------------------------------------------
# Dashboard assembly
# ---------------------------------------------------------------------------

def build_dashboard(orig, cluster, keeper_cluster, n):
    d = {
        "title":    f"ch-customer-{n}-v2",
        "uid":      f"ch-poc-v2-{n}",
        "refresh":  "30s",
        "time":     {"from": "now-1h", "to": "now"},
        "schemaVersion": 36,
        "editable": True,
        "graphTooltip": 1,
        "description": (
            f"ClickHouse metrics for {cluster} / Keeper: {keeper_cluster} "
            f"— sourced from OTel pipeline (migrated from Prometheus dashboard 23285)"
        ),
        "links":    orig.get("links", []),
        "annotations": orig.get("annotations", {}),
        "templating": {"list": []},
        "panels":   [convert_panel(p, cluster, keeper_cluster) for p in orig["panels"]],
    }
    return d

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

with open("23285_rev1.json") as f:
    src = json.load(f)

for n, cluster, keeper in [
    ("1", "ch-customer-1", "ch-keeper-1"),
    ("2", "ch-customer-2", "ch-keeper-2"),
]:
    dash = build_dashboard(src, cluster, keeper, n)
    path = f"grafana/dashboards/ch-customer-{n}-v2.json"
    with open(path, "w") as f:
        json.dump(dash, f, indent=2)
    print(f"Written {path}")
