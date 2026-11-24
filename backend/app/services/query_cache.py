"""查询缓存层领域服务：管理分析查询结果缓存配置、失效触发和版本绑定。

真实数据流：定义缓存配置（关联数据集/缓存时长/失效策略）→ 拦截分析查询
→ 命中时返回缓存版本 → 失效时按最新已确认聚合结果刷新并更新缓存。

一致性约定：
- 缓存数据落在独立的 query_cache_entries 表，与真实指标快照（metric_values）完全隔离，
  即使缓存配置编号与真实指标编号相同，缓存读写也不会污染指标历史；
- 每条缓存绑定 (缓存配置, 查询条件散列)，相同配置下的不同查询条件互不混读；
- 每条缓存记录其依赖的窗口、分组、聚合版本和完成范围（window_start/window_end），
  并保留来源聚合结果 id，支持回核；
- 局部重算确认后只使受影响分组的缓存失效，未受影响分组的缓存继续命中；
- 暂存（未确认）、失败或废弃的重算结果对缓存读写不可见。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.database import db
from app.services.base import BaseModuleService


def mark_stale_for_completion(
    window_id: int,
    dimension_keys: list[str] | None,
    computation_version: int,
) -> int:
    """局部失效：聚合确认后，只把该窗口受影响分组、且版本落后的缓存标记为 stale。

    由实时聚合器在确认分组计算时调用；未涉及分组的缓存条目保持 active，
    重复读取仍然命中。返回被标记的条目数。
    """
    where = "window_id=? AND computation_version<? AND status='active'"
    params: list[Any] = [db.now(), window_id, computation_version]
    if dimension_keys:
        placeholders = ", ".join("?" for _ in dimension_keys)
        where += f" AND dimension_key IN ({placeholders})"
        params.extend(str(d) for d in dimension_keys)
    cursor = db.execute(
        f"UPDATE query_cache_entries SET status='stale', updated_at=? WHERE {where}",
        params,
    )
    return cursor.rowcount


def latest_confirmed_result(window_id: int, dimension_key: str) -> dict[str, Any] | None:
    """取指定窗口+分组最新一条已确认聚合结果（暂存/失败/废弃批次不可见）。"""
    return db.fetchone(
        "SELECT * FROM aggregation_results "
        "WHERE window_id=? AND dimension_key=? AND status='confirmed' "
        "ORDER BY computation_version DESC, id DESC LIMIT 1",
        [window_id, dimension_key],
    )


class QueryCacheService(BaseModuleService):
    MODULE = "query_cache"
    ENTITY = "缓存配置"
    OPERATION_TERM = "缓存"
    LIST_FIELDS = ["配置名称", "关联数据集", "缓存时长秒数", "失效策略", "命中率"]
    REQUIRED_FIELDS = ["配置名称", "关联数据集", "缓存时长秒数"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用缓存": "已启用", "停用缓存": "已停用"}
    NEGATIVE_ACTIONS = ["停用缓存"]

    # ================================================================
    # 缓存配置 CRUD（独立业务表 query_cache_configs）
    # ================================================================

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        try:
            ttl = int(values.get("缓存时长秒数") or 0)
        except (TypeError, ValueError):
            ttl = 0
        entry_id = db.insert("query_cache_configs", {
            "config_name": str(values.get("配置名称", "")).strip(),
            "related_dataset": str(values.get("关联数据集", "")).strip(),
            "cache_ttl_seconds": ttl,
            "invalidation_policy": str(values.get("失效策略") or "version").strip(),
            "hit_rate": 0.0,
            "status": self.STATUS_ORDER[0],
        })
        return db.fetchone("SELECT * FROM query_cache_configs WHERE id=?", [entry_id]), []

    # ================================================================
    # 缓存键与条目定位
    # ================================================================

    def cache_key_for(self, query_params: dict[str, Any]) -> str:
        """生成缓存键：只由查询条件决定。

        条目按 (cache_config_id, cache_key) 唯一归属，因此同一配置下
        查询条件不同则键不同，彼此不得混读；不同配置之间天然隔离。
        """
        raw = json.dumps(query_params or {}, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def _get_config(self, cache_config_id: int) -> dict[str, Any] | None:
        return db.fetchone("SELECT * FROM query_cache_configs WHERE id=?", [cache_config_id])

    @staticmethod
    def _dimension_of(query_params: dict[str, Any]) -> str:
        return str(query_params.get("dimension_key") or "__all__")

    # ================================================================
    # 缓存读取
    # ================================================================

    def get_cached(self, cache_config_id: int, query_params: dict[str, Any]) -> dict[str, Any]:
        """尝试从缓存获取结果。

        命中条件（全部满足）：
        1. 存在 (配置, 查询条件) 对应的 active 条目；
        2. 条目未超出缓存时长；
        3. 条目绑定的聚合版本与该分组最新已确认版本一致。
        """
        cache_key = self.cache_key_for(query_params)
        config = self._get_config(cache_config_id)
        if config is None:
            return {"hit": False, "cache_key": cache_key, "reason": "config_missing"}

        entry = db.fetchone(
            "SELECT * FROM query_cache_entries WHERE cache_config_id=? AND cache_key=?",
            [cache_config_id, cache_key],
        )
        if entry is None:
            self._record_stat(config, hit=False)
            return {"hit": False, "cache_key": cache_key, "reason": "no_entry"}

        stale_reason = self._staleness_reason(config, entry)
        if stale_reason is not None:
            if entry["status"] == "active":
                # 懒失效：版本/TTL 校验不通过时落库标记，保证持久状态一致
                db.update("query_cache_entries",
                          {"status": "stale", "updated_at": db.now()},
                          "id=?", [entry["id"]])
            self._record_stat(config, hit=False)
            return {
                "hit": False,
                "cache_key": cache_key,
                "reason": stale_reason,
                "stale_entry": self._entry_view(entry),
            }

        db.update("query_cache_entries",
                  {"hit_count": entry["hit_count"] + 1, "updated_at": db.now()},
                  "id=?", [entry["id"]])
        self._record_stat(config, hit=True)
        view = self._entry_view(entry)
        return {
            "hit": True,
            "cache_key": cache_key,
            "value": entry["cached_value"],
            **view,
        }

    def _staleness_reason(self, config: dict[str, Any], entry: dict[str, Any]) -> str | None:
        """判断条目是否失效，返回原因；有效返回 None。"""
        if entry["status"] != "active":
            return "invalidated"
        ttl = int(config.get("cache_ttl_seconds") or 0)
        if ttl > 0:
            try:
                updated = datetime.strptime(entry["updated_at"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - updated).total_seconds() > ttl:
                    return "expired"
            except (TypeError, ValueError):
                pass
        if entry["window_id"] is not None:
            source = latest_confirmed_result(entry["window_id"], entry["dimension_key"])
            if source is None:
                return "source_missing"
            if int(source["computation_version"]) != int(entry["computation_version"]):
                return "version_mismatch"
        return None

    # ================================================================
    # 缓存写入与刷新
    # ================================================================

    def set_cached(
        self,
        cache_config_id: int,
        query_params: dict[str, Any],
        value: Any,
        *,
        window_id: int | None = None,
        dimension_key: str | None = None,
        computation_version: int = 0,
        window_start: str | None = None,
        window_end: str | None = None,
        source_result_id: int | None = None,
    ) -> dict[str, Any]:
        """写入（或更新）缓存条目，绑定窗口、查询条件和实际聚合版本。

        只写 query_cache_entries，绝不触碰 metric_values 真实指标快照。
        """
        cache_key = self.cache_key_for(query_params)
        now = db.now()
        data = {
            "query_conditions": json.dumps(query_params or {}, sort_keys=True, ensure_ascii=False, default=str),
            "window_id": window_id,
            "dimension_key": dimension_key or self._dimension_of(query_params),
            "computation_version": int(computation_version),
            "window_start": window_start,
            "window_end": window_end,
            "source_result_id": source_result_id,
            "cached_value": float(value) if isinstance(value, (int, float)) else 0.0,
            "status": "active",
            "updated_at": now,
        }
        existing = db.fetchone(
            "SELECT id FROM query_cache_entries WHERE cache_config_id=? AND cache_key=?",
            [cache_config_id, cache_key],
        )
        if existing:
            db.update("query_cache_entries", data, "id=?", [existing["id"]])
            entry_id = existing["id"]
        else:
            entry_id = db.insert("query_cache_entries", {
                "cache_config_id": cache_config_id,
                "cache_key": cache_key,
                **data,
            })
        return db.fetchone("SELECT * FROM query_cache_entries WHERE id=?", [entry_id])

    def refresh(self, cache_config_id: int, query_params: dict[str, Any]) -> dict[str, Any]:
        """查询入口：先读缓存，失效则按最新已确认聚合结果刷新。

        返回中始终说明实际使用的是哪次结果（聚合版本 / 来源结果 id / 完成范围），
        命中时即缓存所绑定的版本，刷新时即新写入的版本。
        """
        cached = self.get_cached(cache_config_id, query_params)
        if cached["hit"]:
            return {
                "ok": True,
                "hit": True,
                "refreshed": False,
                "value": cached["value"],
                "cache_key": cached["cache_key"],
                "used_result": self._provenance(cached),
            }

        config = self._get_config(cache_config_id)
        if config is None:
            return {"ok": False, "hit": False, "refreshed": False,
                    "message": f"缓存配置 {cache_config_id} 不存在"}

        window_id = query_params.get("window_id")
        if window_id is None:
            return {"ok": False, "hit": False, "refreshed": False,
                    "message": "查询条件缺少 window_id，无法定位聚合结果"}
        dimension_key = self._dimension_of(query_params)

        source = latest_confirmed_result(int(window_id), dimension_key)
        if source is None:
            return {"ok": False, "hit": False, "refreshed": False,
                    "message": f"窗口 {window_id} 分组 {dimension_key} 没有已确认的聚合结果"}

        entry = self.set_cached(
            cache_config_id,
            query_params,
            source["computed_value"],
            window_id=source["window_id"],
            dimension_key=dimension_key,
            computation_version=source["computation_version"],
            window_start=source["window_start"],
            window_end=source["window_end"],
            source_result_id=source["id"],
        )
        return {
            "ok": True,
            "hit": False,
            "refreshed": True,
            "value": entry["cached_value"],
            "cache_key": entry["cache_key"],
            "used_result": self._provenance(entry),
        }

    # ================================================================
    # 失效
    # ================================================================

    def invalidate(self, dataset: str) -> int:
        """使指定数据集名下所有缓存配置的条目失效，返回失效条目数。"""
        configs = db.fetchall(
            "SELECT id FROM query_cache_configs WHERE related_dataset=?", [dataset],
        )
        invalidated = 0
        for cfg in configs:
            invalidated += db.update(
                "query_cache_entries",
                {"status": "stale", "updated_at": db.now()},
                "cache_config_id=? AND status='active'",
                [cfg["id"]],
            )
        return invalidated

    def invalidate_window(
        self,
        window_id: int,
        dimension_keys: list[str] | None = None,
    ) -> int:
        """按窗口局部失效：仅指定分组（缺省为窗口全部分组）的缓存失效。"""
        return mark_stale_for_completion(window_id, dimension_keys, 1 << 30)

    def invalidate_window_for_config(self, cache_config_id: int) -> int:
        """使指定配置下的全部缓存条目失效，返回失效条目数。"""
        return db.update(
            "query_cache_entries",
            {"status": "stale", "updated_at": db.now()},
            "cache_config_id=? AND status='active'",
            [cache_config_id],
        )

    # ================================================================
    # 统计与回核
    # ================================================================

    def entries(self, cache_config_id: int) -> list[dict[str, Any]]:
        """列出配置下的缓存条目（含来源聚合结果与版本），用于回核。"""
        return db.fetchall(
            "SELECT * FROM query_cache_entries WHERE cache_config_id=? ORDER BY id",
            [cache_config_id],
        )

    def hit_rate(self, cache_config_id: int) -> dict[str, Any]:
        """统计缓存命中率（基于真实命中/未命中计数）。"""
        config = self._get_config(cache_config_id)
        if config is None:
            return {"cache_config_id": cache_config_id, "error": "config_missing"}
        hits = int(config["hit_count"])
        misses = int(config["miss_count"])
        total = hits + misses
        return {
            "cache_config_id": cache_config_id,
            "hits": hits,
            "misses": misses,
            "hit_rate": round(hits / total, 4) if total else 0.0,
            "total_cached_queries": db.count(
                "query_cache_entries", "cache_config_id=?", [cache_config_id]),
        }

    def _record_stat(self, config: dict[str, Any], *, hit: bool) -> None:
        hits = int(config["hit_count"]) + (1 if hit else 0)
        misses = int(config["miss_count"]) + (0 if hit else 1)
        total = hits + misses
        db.update("query_cache_configs", {
            "hit_count": hits,
            "miss_count": misses,
            "hit_rate": round(hits / total, 4) if total else 0.0,
        }, "id=?", [config["id"]])
        config["hit_count"], config["miss_count"] = hits, misses

    @staticmethod
    def _entry_view(entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "window_id": entry["window_id"],
            "dimension_key": entry["dimension_key"],
            "computation_version": entry["computation_version"],
            "window_start": entry["window_start"],
            "window_end": entry["window_end"],
            "source_result_id": entry["source_result_id"],
            "status": entry["status"],
        }

    def _provenance(self, entry: dict[str, Any]) -> dict[str, Any]:
        """实际使用的是哪次结果：聚合版本 + 完成范围 + 来源结果 id。"""
        return {
            "result_id": entry.get("source_result_id"),
            "computation_version": entry.get("computation_version"),
            "window_id": entry.get("window_id"),
            "dimension_key": entry.get("dimension_key"),
            "window_start": entry.get("window_start"),
            "window_end": entry.get("window_end"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["active_cache_configs"] = db.count("query_cache_configs", "status='已启用'")
        snapshot["active_entries"] = db.count("query_cache_entries", "status='active'")
        snapshot["stale_entries"] = db.count("query_cache_entries", "status='stale'")
        return snapshot
