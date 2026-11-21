"""成组多源采集路由：多选来源一次提交、按源分批、成组回执与补送。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.group_ingest import group_ingest_service

router = APIRouter(prefix="/api/group-ingest", tags=["成组采集"])


class GroupSourcePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)


class GroupSubmitPayload(BaseModel):
    group_no: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    sources: list[GroupSourcePayload] = Field(..., min_length=1)


class GroupSupplementPayload(BaseModel):
    group_no: str = Field(..., min_length=1)
    sources: list[GroupSourcePayload] = Field(..., min_length=1)
    time_window: str | None = None


class GroupRetryPayload(BaseModel):
    group_no: str = Field(..., min_length=1)
    source_id: str = Field(..., min_length=1)


@router.post("/submit")
def submit_group(payload: GroupSubmitPayload) -> dict[str, Any]:
    """多选来源一次提交，逐条按实际来源分批接收，返回成组采集回执。"""
    return group_ingest_service.submit_group(
        payload.group_no,
        [item.model_dump() for item in payload.sources],
        payload.time_window,
    )


@router.post("/supplement")
def supplement_group(payload: GroupSupplementPayload) -> dict[str, Any]:
    """凭证通过后用原组编号补送尚缺来源。"""
    return group_ingest_service.supplement_group(
        payload.group_no,
        [item.model_dump() for item in payload.sources],
        payload.time_window,
    )


@router.get("/receipt/{group_no}")
def get_group_receipt(group_no: str) -> dict[str, Any]:
    """查看成组采集回执：各批号、缺失来源、正式记录可见性。"""
    return group_ingest_service.get_group_receipt(group_no)


@router.get("/formal/{group_no}")
def get_formal_records(group_no: str) -> dict[str, Any]:
    """齐备后可见的正式记录；缺路时明确告知尚缺哪路。"""
    return group_ingest_service.get_formal_records(group_no)


@router.get("/partial/{group_no}")
def get_partial_raw(group_no: str) -> dict[str, Any]:
    """缺路期间先利用已接收原文，同时确认尚缺来源与暂存原文。"""
    return group_ingest_service.get_partial_raw(group_no)


@router.get("/pending")
def list_pending(group_no: str | None = None) -> dict[str, Any]:
    """本组待处理清单：回写处理结果并关联每个源的原始行。"""
    return group_ingest_service.list_pending(group_no)


@router.post("/retry-slot")
def retry_failed_slot(payload: GroupRetryPayload) -> dict[str, Any]:
    """个别来源故障时定位需要重做的那一批并重试。"""
    return group_ingest_service.retry_failed_slot(payload.group_no, payload.source_id)
