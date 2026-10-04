"""回网三向合并：按稳定编号逐字段合并中心版、站点版与共同基线。

只有中心和站点相对共同基线都修改过同一字段才算冲突；
一方未动则直接采用另一方的值。测量值和处置两版都保留，
由稳定编号上的 divergent 标记提示人工选定，选定前事件不可关闭。
"""

# 内部记账字段不参与逐字段合并
RESERVED_FIELDS = {
    "divergent",
    "divergent_fields",
    "branches",
    "late_revision",
    "revised_by",
    "resolved_by",
    "completed_by",
}

# 冲突时自动取值优先级明确的字段
NUMERIC_WIN_FIELDS = {"revision", "severity_score"}
TIMESTAMP_WIN_FIELDS = {"observed_at", "last_seen"}
SEVERITY_ORDER = {"low": 1, "medium": 2, "high": 3, "critical": 4}


def _numeric(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def auto_winner(field, center_value, station_value):
    """可自动裁决的字段返回胜者，否则返回 None 表示保留两版。"""
    if field == "revision":
        c, s = _numeric(center_value), _numeric(station_value)
        if c is not None and s is not None and c != s:
            return "station" if s > c else "center"
    if field in NUMERIC_WIN_FIELDS:
        c, s = _numeric(center_value), _numeric(station_value)
        if c is not None and s is not None and c != s:
            return "station" if s > c else "center"
    if field == "severity" and center_value in SEVERITY_ORDER and station_value in SEVERITY_ORDER:
        if center_value != station_value:
            return "station" if SEVERITY_ORDER[station_value] > SEVERITY_ORDER[center_value] else "center"
    if field in TIMESTAMP_WIN_FIELDS:
        if center_value != station_value:
            return "station" if str(station_value) > str(center_value) else "center"
    return None


def three_way_merge(base, center, station):
    """对三份字段字典做三向合并。

    返回 (merged, conflicts, auto_take)：
    - merged：逐字段合并结果；
    - conflicts：双方都改且无法自动裁决的字段 -> {"center": v, "station": v}；
    - auto_take：自动裁断字段 -> 采用方。
    """
    merged = dict(center)
    conflicts = {}
    auto_take = {}
    base = base or {}
    fields = set(center) | set(station)
    for field in fields:
        if field in RESERVED_FIELDS:
            continue
        base_value = base.get(field)
        center_value = center.get(field)
        station_value = station.get(field)
        center_changed = center_value != base_value
        station_changed = station_value != base_value
        if not station_changed:
            continue
        if not center_changed:
            if field in station:
                merged[field] = station_value
            continue
        if center_value == station_value:
            merged[field] = center_value
            continue
        winner = auto_winner(field, center_value, station_value)
        if winner:
            merged[field] = station_value if winner == "station" else center_value
            auto_take[field] = winner
        else:
            merged[field] = center_value
            conflicts[field] = {"center": center_value, "station": station_value}
    # 清理已不存在的字段
    for field in list(merged):
        if field in RESERVED_FIELDS:
            continue
        if field not in center and field not in station:
            merged.pop(field, None)
    return merged, conflicts, auto_take


def merge_status(kind, base_status, center_status, station_status, rules):
    """状态字段合并：未动则取他方；双方分叉且不可一步到达时保留中心状态。"""
    if station_status == base_status:
        return center_status, False
    if center_status == base_status:
        return station_status, False
    if center_status == station_status:
        return center_status, False
    if rules.can_reach(kind, center_status, station_status):
        return station_status, False
    return center_status, True
