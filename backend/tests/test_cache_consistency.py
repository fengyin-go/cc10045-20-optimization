"""缓存持久读写一致性场景验收测试。

场景（对应需求）：
1. 同一窗口两个分组（线上/门店），线上数据更新，门店分组不变；
2. 线上一次重算失败 → 对外可用结果不变；重试暂存后，确认与查询交错：
   确认前查询仍取旧数，确认后线上不得再取旧数；
3. 门店无需重算，既有缓存继续命中，且来源（聚合版本/结果 id）可回核；
4. 同一缓存配置下不同查询条件互不混读；
5. 缓存写入独立于真实指标快照：编号与真实指标相同的缓存配置，
   其写入不会让指标历史多出新值。

运行：cd backend && .venv/bin/python tests/test_cache_consistency.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

# 独立临时数据库，避免污染开发库；必须在导入 app 之前设置
os.environ["CC10045_DB_DIR"] = tempfile.mkdtemp(prefix="cache_consistency_")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import db  # noqa: E402
from app.services import service_for  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}  {detail}")


def add_events(channel: str, n: int) -> None:
    # 聚合窗口为半开区间 [start, end)，事件时间需略早于当前时刻
    from datetime import datetime, timedelta, timezone
    past = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%S")
    db.executemany(
        "INSERT INTO events(event_name,event_attrs,trigger_condition,source_module,"
        "occurred_at,batch_id,payload_size) VALUES(?,?,?,?,?,?,?)",
        [("下单", json.dumps({"channel": channel}, ensure_ascii=False), "",
          "event_tracker", past, "T_CACHE", 128) for _ in range(n)],
    )


def main() -> int:
    agg = service_for("realtime_agg")
    cache = service_for("query_cache")

    # ------------------------------------------------------------------
    # 准备：一个按 channel 分组的窗口；线上 40、门店 60
    # ------------------------------------------------------------------
    print("== 准备：窗口 + 两组事件（线上40 / 门店60） ==")
    window_id = db.insert("aggregation_windows", {
        "window_name": "渠道分群窗口",
        "data_source_module": "event_tracker",
        "dimensions": json.dumps(["channel"], ensure_ascii=False),
        "agg_function": "COUNT",
        "window_type": "tumbling",
        "window_size_seconds": 86400,
        "status": "已启用",
    })
    add_events("线上", 40)
    add_events("门店", 60)

    run = agg.run_aggregation(window_id)
    check("初始聚合确认成功", run["ok"] and run["completion"] is not None)
    check("初始版本为 1", run["completion"]["computation_version"] == 1)
    check("完成范围覆盖两个分组",
          sorted(run["completion"]["confirmed_dimensions"]) == ["线上", "门店"])

    # 缓存配置（编号将与真实指标相同）+ 真实指标
    metric_id = db.insert("metrics", {
        "metric_name": "渠道订单量", "metric_type": "counter",
        "source_module": "realtime_agg", "calibration_version": 1, "unit": "单",
    })
    config, missing = cache.create_entry(
        {"配置名称": "渠道查询缓存", "关联数据集": "channel_ds", "缓存时长秒数": 300})
    check("缓存配置创建成功", config is not None and not missing)
    config_id = config["id"]
    cache.run_action(config_id, "启用缓存")
    check("缓存配置编号恰与真实指标相同", config_id == metric_id,
          f"config={config_id} metric={metric_id}")

    q_online = {"window_id": window_id, "dimension_key": "线上"}
    q_store = {"window_id": window_id, "dimension_key": "门店"}

    # ------------------------------------------------------------------
    # 阶段 1：首次查询刷新缓存；重复读取命中；不同查询条件不混读
    # ------------------------------------------------------------------
    print("== 阶段1：首次查询 线上40 / 门店60，重复读取命中 ==")
    r1 = cache.refresh(config_id, q_online)
    check("线上首次未命中并刷新", r1["ok"] and not r1["hit"] and r1["refreshed"])
    check("线上取值 40", r1["value"] == 40.0, f"got {r1['value']}")
    check("返回实际使用的版本 v1", r1["used_result"]["computation_version"] == 1)
    check("使用结果带来源 id 与完成范围",
          r1["used_result"]["result_id"] is not None
          and r1["used_result"]["window_start"] and r1["used_result"]["window_end"])

    r1b = cache.refresh(config_id, q_online)
    check("线上重复读取命中", r1b["hit"] and not r1b["refreshed"] and r1b["value"] == 40.0)

    r2 = cache.refresh(config_id, q_store)
    check("门店首次刷新取值 60", r2["value"] == 60.0, f"got {r2['value']}")

    entries = cache.entries(config_id)
    check("同配置两个查询条件各占一条缓存", len(entries) == 2)
    check("不同查询条件缓存键不同",
          entries[0]["cache_key"] != entries[1]["cache_key"])
    check("门店缓存未被线上污染（60 而非 40）",
          next(e for e in entries if e["dimension_key"] == "门店")["cached_value"] == 60.0)

    # ------------------------------------------------------------------
    # 阶段 2：线上数据更新为 42；第一次重算失败 → 对外结果不变
    # ------------------------------------------------------------------
    print("== 阶段2：线上一次重算失败，对外仍取旧数 ==")
    add_events("线上", 2)  # 线上 40 -> 42
    staged_fail = agg.stage_recompute(window_id, ["线上"])
    check("局部暂存只涉及线上分组",
          staged_fail["ok"] and [s["dimension_key"] for s in staged_fail["staged"]] == ["线上"])
    failed = agg.fail_recompute(window_id, staged_fail["computation_version"], "模拟计算失败")
    check("失败标记生效", failed["ok"] and failed["discarded"] == 1)

    r3 = cache.refresh(config_id, q_online)
    check("失败重算后线上仍命中旧缓存 40", r3["hit"] and r3["value"] == 40.0)
    latest = agg.latest_value(window_id, "线上")
    check("失败批次对外不可见（最新已确认仍 40）",
          latest is not None and latest["computed_value"] == 40.0)

    # ------------------------------------------------------------------
    # 阶段 3：重试暂存 42，确认与查询交错 —— 确认前仍取旧数
    # ------------------------------------------------------------------
    print("== 阶段3：重试暂存 42，确认前查询仍取旧数 ==")
    staged = agg.stage_recompute(window_id, ["线上"])
    check("重试暂存值为 42",
          staged["ok"] and staged["staged"][0]["computed_value"] == 42.0,
          f"got {staged['staged'][0]['computed_value'] if staged['staged'] else None}")
    version_new = staged["computation_version"]

    r4 = cache.refresh(config_id, q_online)
    check("确认前线上仍命中旧缓存 40", r4["hit"] and r4["value"] == 40.0)
    r5 = cache.refresh(config_id, q_store)
    check("确认前门店仍命中 60", r5["hit"] and r5["value"] == 60.0)

    # ------------------------------------------------------------------
    # 阶段 4：确认 → 线上不得再取旧数；门店无需重算且来源可回核
    # ------------------------------------------------------------------
    print("== 阶段4：确认后线上刷新为 42，门店保持命中 ==")
    confirmed = agg.confirm_recompute(window_id, version_new)
    completion = confirmed["completion"]
    check("确认返回完成范围（版本/分组/窗口区间）",
          confirmed["ok"]
          and completion["computation_version"] == version_new
          and completion["confirmed_dimensions"] == ["线上"]
          and completion["window_start"] and completion["window_end"])
    check("确认只局部失效受影响分组（1 条）",
          completion["invalidated_cache_entries"] == 1,
          f"got {completion['invalidated_cache_entries']}")

    r6 = cache.refresh(config_id, q_online)
    check("确认后线上不再取旧数", not r6["hit"] and r6["refreshed"])
    check("确认后线上读取 42", r6["value"] == 42.0, f"got {r6['value']}")
    check("刷新返回实际使用的新版本", r6["used_result"]["computation_version"] == version_new)

    r7 = cache.refresh(config_id, q_store)
    check("门店无需重算仍命中 60", r7["hit"] and r7["value"] == 60.0)

    entries = cache.entries(config_id)
    e_store = next(e for e in entries if e["dimension_key"] == "门店")
    e_online = next(e for e in entries if e["dimension_key"] == "线上")
    check("门店缓存来源可回核（仍为 v1 结果）",
          e_store["computation_version"] == 1 and e_store["source_result_id"] is not None)
    src = db.fetchone("SELECT * FROM aggregation_results WHERE id=?", [e_store["source_result_id"]])
    check("门店来源结果确实存在且值一致",
          src is not None and src["computed_value"] == 60.0 and src["status"] == "confirmed")
    check("线上缓存已绑定新版本", e_online["computation_version"] == version_new)

    # ------------------------------------------------------------------
    # 阶段 5：缓存数据独立于真实指标快照
    # ------------------------------------------------------------------
    print("== 阶段5：缓存写入不污染真实指标历史 ==")
    mv_count = db.count("metric_values", "metric_id=?", [metric_id])
    check("指标历史没有新增快照", mv_count == 0, f"metric_values rows={mv_count}")
    mv_42 = db.count("metric_values", "metric_id=? AND snapshot_value=?", [metric_id, 42.0])
    check("指标历史没有多出 42", mv_42 == 0)
    check("缓存数据落在独立表",
          db.count("query_cache_entries", "cache_config_id=?", [config_id]) == 2)

    stats = cache.hit_rate(config_id)
    check("命中率统计基于真实计数", stats["hits"] > 0 and stats["hit_rate"] > 0)

    print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
