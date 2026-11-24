"""实时聚合器领域服务：管理聚合窗口配置并驱动真实聚合计算。

真实数据流：接收事件定义 → 按窗口维度分组 → 执行 COUNT/SUM/AVG/MAX/MIN 等聚合函数
→ 结果写入 aggregation_results 表 → 供下游指标存储和仪表盘消费。

窗口类型支持 tumbling（滚动）、sliding（滑动）、session（会话）三种，
每种类型有独立的时间语义和结果合并策略。

重算生命周期：stage（暂存）→ confirm（确认）/ fail（失败）。
只有已确认（confirmed）的分组结果对外可见；暂存、失败、废弃的批次
不改变任何对外可用结果。确认时返回本次的完成范围（窗口区间 + 聚合版本 +
涉及分组），供查询缓存层校验和局部失效。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.database import db
from app.pipeline import compute_aggregation
from app.services.base import BaseModuleService
from app.services.query_cache import mark_stale_for_completion


class RealtimeAggService(BaseModuleService):
    MODULE = "realtime_agg"
    ENTITY = "聚合窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口名称", "数据源", "聚合维度", "聚合函数", "窗口大小"]
    REQUIRED_FIELDS = ["窗口名称", "数据源", "聚合维度"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用聚合": "已启用", "停用聚合": "已停用"}
    NEGATIVE_ACTIONS = ["停用聚合"]

    # ================================================================
    # 扩展查询
    # ================================================================

    def list_windows(
        self, status: str | None = None, data_source: str | None = None,
        page: int = 1, size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """列出聚合窗口配置，支持按数据源筛选。"""
        where = "1=1"
        params: list[Any] = []
        if status:
            where += " AND status=?"
            params.append(status)
        if data_source:
            where += " AND data_source_module=?"
            params.append(data_source)

        total = db.count("aggregation_windows", where, params)
        rows = db.fetchall(
            f"SELECT * FROM aggregation_windows WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [size, (page - 1) * size],
        )
        return _sqlite_rows_to_dict(rows), total

    def results_by_window(
        self, window_id: int, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """查询指定窗口的聚合结果（仅已确认）。按窗口起始时间倒序。"""
        rows = db.fetchall(
            "SELECT * FROM aggregation_results WHERE window_id=? AND status='confirmed' "
            "ORDER BY window_start DESC LIMIT ?",
            [window_id, limit],
        )
        return _sqlite_rows_to_dict(rows)

    def latest_value(
        self, window_id: int, dimension_key: str = "__all__",
    ) -> dict[str, Any] | None:
        """获取指定窗口和维度的最新已确认聚合值。"""
        return db.fetchone(
            "SELECT * FROM aggregation_results WHERE window_id=? AND dimension_key=? "
            "AND status='confirmed' ORDER BY computation_version DESC, id DESC LIMIT 1",
            [window_id, dimension_key],
        )

    # ================================================================
    # 聚合执行
    # ================================================================

    def run_aggregation(self, window_id: int) -> dict[str, Any]:
        """对指定窗口执行实时聚合计算（暂存 + 确认一步完成）。

        1. 从 aggregation_windows 加载窗口配置
        2. 调用 pipeline.compute_aggregation 执行计算并直接确认
        3. 返回本次计算的结果摘要与完成范围（供缓存层校验）
        """
        window = db.fetchone("SELECT * FROM aggregation_windows WHERE id=?", [window_id])
        if window is None:
            return {"ok": False, "message": f"窗口 {window_id} 不存在", "results": []}

        if window["status"] != "已启用":
            return {"ok": False, "message": f"窗口 {window['window_name']} 未启用，当前状态: {window['status']}", "results": []}

        results = compute_aggregation(dict(window))
        completion = self._confirm_side_effects(window_id, results)

        return {
            "ok": True,
            "message": f"窗口 {window['window_name']} 完成聚合，产出 {len(results)} 条结果",
            "results": results,
            "window_name": window["window_name"],
            "window_type": window["window_type"],
            "total_value": sum(r["computed_value"] for r in results),
            "completion": completion,
        }

    # ================================================================
    # 重算生命周期：stage → confirm / fail
    # ================================================================

    def stage_recompute(
        self, window_id: int, dimension_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        """暂存一次（局部）重算：计算结果以 pending 状态落库，确认前对外不可见。

        - ``dimension_keys`` 限定时只重算指定分组，其余分组保持已确认版本；
        - 同一窗口此前的未确认批次会被废弃（abandoned），不影响对外结果。
        """
        window = db.fetchone("SELECT * FROM aggregation_windows WHERE id=?", [window_id])
        if window is None:
            return {"ok": False, "message": f"窗口 {window_id} 不存在", "staged": []}
        if window["status"] != "已启用":
            return {"ok": False, "message": f"窗口 {window['window_name']} 未启用，当前状态: {window['status']}", "staged": []}

        # 废弃该窗口尚未确认的旧批次，保证最多一个待确认批次
        db.execute(
            "UPDATE aggregation_results SET status='abandoned' WHERE window_id=? AND status='pending'",
            [window_id],
        )

        results = compute_aggregation(dict(window), dimension_keys=dimension_keys, status="pending")
        if not results:
            return {"ok": False, "message": "窗口内无匹配数据，未产生待确认结果", "staged": []}

        version = results[0]["computation_version"]
        return {
            "ok": True,
            "message": f"窗口 {window['window_name']} 已暂存版本 {version}（{len(results)} 个分组），待确认",
            "window_id": window_id,
            "computation_version": version,
            "staged": results,
            "window_start": results[0]["window_start"],
            "window_end": results[0]["window_end"],
        }

    def confirm_recompute(
        self, window_id: int, computation_version: int | None = None,
    ) -> dict[str, Any]:
        """确认一个暂存批次：置为 confirmed 并返回缓存数据依赖的完成范围。

        确认后只使受影响分组的缓存失效；未涉及分组的既有缓存继续命中。
        """
        if computation_version is None:
            row = db.fetchone(
                "SELECT MAX(computation_version) as v FROM aggregation_results "
                "WHERE window_id=? AND status='pending'",
                [window_id],
            )
            computation_version = int(row["v"]) if row and row["v"] is not None else None
        if computation_version is None:
            return {"ok": False, "message": f"窗口 {window_id} 没有待确认的重算批次"}

        pending = db.fetchall(
            "SELECT * FROM aggregation_results WHERE window_id=? AND computation_version=? AND status='pending'",
            [window_id, computation_version],
        )
        if not pending:
            return {"ok": False, "message": f"窗口 {window_id} 版本 {computation_version} 没有待确认结果"}

        db.execute(
            "UPDATE aggregation_results SET status='confirmed' "
            "WHERE window_id=? AND computation_version=? AND status='pending'",
            [window_id, computation_version],
        )

        completion = self._confirm_side_effects(window_id, pending)
        return {
            "ok": True,
            "message": f"窗口 {window_id} 版本 {computation_version} 已确认（{len(pending)} 个分组）",
            "window_id": window_id,
            "computation_version": computation_version,
            "confirmed_count": len(pending),
            "completion": completion,
        }

    def fail_recompute(
        self, window_id: int, computation_version: int | None = None, reason: str = "",
    ) -> dict[str, Any]:
        """标记一个暂存批次为失败：直接废弃，对外可用结果不变。"""
        if computation_version is None:
            row = db.fetchone(
                "SELECT MAX(computation_version) as v FROM aggregation_results "
                "WHERE window_id=? AND status='pending'",
                [window_id],
            )
            computation_version = int(row["v"]) if row and row["v"] is not None else None
        if computation_version is None:
            return {"ok": False, "message": f"窗口 {window_id} 没有待确认的重算批次", "discarded": 0}

        discarded = db.update(
            "aggregation_results",
            {"status": "failed"},
            "window_id=? AND computation_version=? AND status='pending'",
            [window_id, computation_version],
        )
        return {
            "ok": discarded > 0,
            "message": f"窗口 {window_id} 版本 {computation_version} 已标记失败（{reason or '未说明'}），对外结果不变",
            "window_id": window_id,
            "computation_version": computation_version,
            "discarded": discarded,
        }

    def completion_range(self, window_id: int, dimension_key: str) -> dict[str, Any] | None:
        """指定分组当前已确认的完成范围（缓存数据所依赖的版本与窗口区间）。"""
        row = self.latest_value(window_id, dimension_key)
        if row is None:
            return None
        return {
            "window_id": row["window_id"],
            "dimension_key": row["dimension_key"],
            "computation_version": row["computation_version"],
            "window_start": row["window_start"],
            "window_end": row["window_end"],
        }

    def _confirm_side_effects(
        self, window_id: int, confirmed_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """确认后的联动：组装完成范围，并局部失效受影响分组的查询缓存。"""
        if not confirmed_rows:
            return None
        version = max(int(r["computation_version"]) for r in confirmed_rows)
        dims = sorted({r["dimension_key"] for r in confirmed_rows})
        invalidated = mark_stale_for_completion(window_id, dims, version)
        return {
            "window_id": window_id,
            "computation_version": version,
            "window_start": min(r["window_start"] for r in confirmed_rows),
            "window_end": max(r["window_end"] for r in confirmed_rows),
            "confirmed_dimensions": dims,
            "invalidated_cache_entries": invalidated,
        }

    def run_all_active(self) -> dict[str, Any]:
        """对所有已启用窗口执行聚合。"""
        windows = db.fetchall("SELECT * FROM aggregation_windows WHERE status='已启用'")
        summary = []
        total_results = 0
        for w in windows:
            r = self.run_aggregation(w["id"])
            summary.append({"window_name": w["window_name"], "result_count": len(r["results"])})
            total_results += len(r["results"])
        return {"ok": True, "windows_processed": len(windows), "total_results": total_results, "summary": summary}

    def time_range_analysis(
        self, window_id: int, start_time: str, end_time: str,
    ) -> dict[str, Any]:
        """对指定时间范围的聚合结果做趋势分析。"""
        rows = db.fetchall(
            "SELECT * FROM aggregation_results WHERE window_id=? AND window_start>=? AND window_start<=? "
            "AND status='confirmed' ORDER BY window_start ASC",
            [window_id, start_time, end_time],
        )
        if not rows:
            return {"trend": "no_data", "points": []}

        values = [r["computed_value"] for r in rows]
        points = [{"time": r["window_start"], "value": r["computed_value"], "count": r["event_count"]} for r in rows]

        import statistics
        mean_v = statistics.mean(values)
        std_v = statistics.stdev(values) if len(values) > 1 else 0
        trend = "stable"
        if len(values) >= 2:
            if values[0] < values[-1] * 0.7:
                trend = "upward"
            elif values[0] > values[-1] * 1.3:
                trend = "downward"

        return {
            "trend": trend,
            "mean": round(mean_v, 4),
            "std": round(std_v, 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "points": points,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        enabled = db.count("aggregation_windows", "status='已启用'")
        total_events = db.count("events")
        total_results = db.count("aggregation_results", "status='confirmed'")
        pending_results = db.count("aggregation_results", "status='pending'")
        snapshot["enabled_windows"] = enabled
        snapshot["total_ingested_events"] = total_events
        snapshot["total_agg_results"] = total_results
        snapshot["pending_agg_results"] = pending_results
        snapshot["latest_batch"] = db.fetchone("SELECT batch_id, MAX(ingested_at) as ts FROM events")
        return snapshot


def _sqlite_rows_to_dict(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return rows