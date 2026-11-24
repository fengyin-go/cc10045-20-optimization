"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body

from app.database import db
from app.pipeline import (
    check_alert, compute_aggregation, compute_cohort_comparison,
    compute_forecast, compute_funnel, compute_metric_snapshot,
    compute_retention, compute_segment, generate_report,
    ingest_events, precompute_cube, seed_real_data,
)
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/aggregation/{window_id}/stage")
def api_stage_aggregation(window_id: int, payload: dict[str, Any] = Body(default_factory=dict)) -> dict:
    """暂存一次（局部）重算：结果确认前对外不可见。

    body 可选 ``dimension_keys``（分组列表）做局部重算，缺省重算全部分组。
    """
    svc = service_for("realtime_agg")
    return svc.stage_recompute(window_id, dimension_keys=payload.get("dimension_keys"))


@router.post("/aggregation/{window_id}/confirm")
def api_confirm_aggregation(window_id: int, payload: dict[str, Any] = Body(default_factory=dict)) -> dict:
    """确认暂存的重算批次，返回缓存数据依赖的完成范围。"""
    svc = service_for("realtime_agg")
    return svc.confirm_recompute(window_id, computation_version=payload.get("computation_version"))


@router.post("/aggregation/{window_id}/fail")
def api_fail_aggregation(window_id: int, payload: dict[str, Any] = Body(default_factory=dict)) -> dict:
    """标记暂存批次失败：废弃本批结果，对外可用结果不变。"""
    svc = service_for("realtime_agg")
    return svc.fail_recompute(
        window_id,
        computation_version=payload.get("computation_version"),
        reason=payload.get("reason", ""),
    )


@router.post("/cache/{config_id}/query")
def api_cache_query(config_id: int, payload: dict[str, Any] = Body(default_factory=dict)) -> dict:
    """按缓存配置 + 查询条件读取分析结果。

    命中返回缓存版本；失效时按最新已确认聚合结果刷新。
    返回中 ``used_result`` 说明实际使用的是哪次结果（聚合版本/完成范围/来源 id）。
    """
    svc = service_for("query_cache")
    query_params = payload.get("query") or payload.get("query_params") or {}
    return svc.refresh(config_id, query_params)


@router.get("/cache/{config_id}/entries")
def api_cache_entries(config_id: int) -> dict:
    """列出配置下的缓存条目（含来源聚合版本与结果 id），用于回核。"""
    svc = service_for("query_cache")
    items = svc.entries(config_id)
    return {"items": items, "total": len(items)}


@router.post("/cache/{config_id}/invalidate")
def api_cache_invalidate(config_id: int, payload: dict[str, Any] = Body(default_factory=dict)) -> dict:
    """失效缓存：按窗口（可限定分组）局部失效，或按数据集整体失效。"""
    svc = service_for("query_cache")
    if payload.get("window_id") is not None:
        count = svc.invalidate_window(int(payload["window_id"]), payload.get("dimension_keys"))
    elif payload.get("dataset"):
        count = svc.invalidate(str(payload["dataset"]))
    else:
        count = svc.invalidate_window_for_config(config_id)
    return {"ok": True, "invalidated": count}


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(funnel_id: int) -> dict:
    """对指定漏斗执行分析计算。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id)


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(task_id: int) -> dict:
    """对指定预测任务执行计算。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/report/{report_id}/generate")
def api_generate_report(report_id: int) -> dict:
    """对指定报告模板执行生成。"""
    svc = service_for("data_report")
    return svc.generate(report_id)


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}