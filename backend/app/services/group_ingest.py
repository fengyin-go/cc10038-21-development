"""成组多源批量采集领域。

一次提交允许多个数据源（多选来源），后端逐条按实际来源分批接收：

- 每个来源在组内拥有独立的「分批身份」(slot)，对应一个 INGE 采集批次；
- 凭证未就绪的来源不建立批次、不落原始行，slot 停在「待补交」，其已上报的
  原文挂在 slot 上暂存，凭证通过后用原组编号补送再入库；
- 数据源登记表按成组回执记录每个来源被占用的「接入资格」；
- 组待处理清单按源回写处理结果，并关联每个源自己的原始行；
- 同一个成组编号重复提交时只认已经建立的分批身份，已完成的来源幂等跳过，
  每条原始日志只归属自己的来源，不会串组串源；
- 整组在全部来源齐备后才出具可见的正式记录，缺路期间已接收原文仍可查询，
  且能确认尚缺哪一路。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.store import store

# 组 / 单路 / 组整体的状态
SLOT_PENDING = "待补交"      # 凭证未就绪，等待该来源补送
SLOT_RECEIVED = "已接收"     # 该来源分批已建立、原始行已入库
SLOT_FAILED = "接收失败"     # 来源侧故障，需要按批重做
GROUP_PARTIAL = "部分接收"   # 组内至少一路未齐备
GROUP_COMPLETE = "已齐备"    # 全部来源均已接收，正式记录可见

GROUP_MODULE = "group_ingest"
PENDING_MODULE = "group_pending"
SLOT_MODULE = "group_slot"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class GroupIngestService:
    """成组采集编排：组 → 来源分批(slot) → 采集批次/原始行。"""

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _find_source(self, source_id: str) -> dict[str, Any] | None:
        for s in store.rows("source_registry"):
            if s.get("来源标识") == source_id:
                return s
        return None

    def _find_group(self, group_no: str) -> dict[str, Any] | None:
        for g in store.rows(GROUP_MODULE):
            if g.get("成组编号") == group_no:
                return g
        return None

    def _slots(self, group_no: str) -> list[dict[str, Any]]:
        return [
            s for s in store.rows(SLOT_MODULE)
            if s.get("成组编号") == group_no
        ]

    def _slot_of(self, group_no: str, source_id: str) -> dict[str, Any] | None:
        for s in self._slots(group_no):
            if s.get("来源标识") == source_id:
                return s
        return None

    @staticmethod
    def _credential_ready(source: dict[str, Any]) -> bool:
        """凭证就绪：数据源已登记且通过连通验证（状态=已验证）。"""
        return source.get("status") == "已验证"

    def _refresh_group_state(self, group: dict[str, Any]) -> None:
        """根据各 slot 状态重算组状态与待处理清单。"""
        slots = self._slots(group["成组编号"])
        received = [s for s in slots if s.get("slot_status") == SLOT_RECEIVED]
        missing = [
            s for s in slots
            if s.get("slot_status") in (SLOT_PENDING, SLOT_FAILED)
        ]
        group["已接收来源数"] = str(len(received))
        group["缺失来源"] = json.dumps(
            [s.get("来源标识") for s in missing], ensure_ascii=False
        )
        if not missing:
            group["组状态"] = GROUP_COMPLETE
            group["status"] = GROUP_COMPLETE
            group["pending"] = False
            group["abnormal"] = False
            group["齐备时间"] = _now_iso()
        else:
            group["组状态"] = GROUP_PARTIAL
            group["status"] = GROUP_PARTIAL
            group["pending"] = True
            group["abnormal"] = any(s.get("slot_status") == SLOT_FAILED for s in missing)

        # 回写本组待处理清单：每个来源一条，关联各自分批与原始行
        pending_rows = store.rows(PENDING_MODULE)
        for slot in slots:
            pending_entry = None
            for p in pending_rows:
                if p.get("成组编号") == group["成组编号"] and p.get("来源标识") == slot["来源标识"]:
                    pending_entry = p
                    break
            values = {
                "成组编号": group["成组编号"],
                "来源标识": slot["来源标识"],
                "分批编号": slot.get("批次编号", ""),
                "处理状态": slot.get("slot_status"),
                "待补原因": slot.get("原因", ""),
                "关联原始行": slot.get("raw_record_ids", "[]"),
            }
            if pending_entry is None:
                pending_entry = {"id": _next_id(PENDING_MODULE)}
                pending_entry.update(values)
                pending_entry.update({
                    "status": slot.get("slot_status"),
                    "pending": slot.get("slot_status") != SLOT_RECEIVED,
                    "abnormal": slot.get("slot_status") == SLOT_FAILED,
                    "created_at": _now_iso(),
                })
                pending_rows.append(pending_entry)
            else:
                pending_entry.update(values)
                pending_entry["status"] = slot.get("slot_status")
                pending_entry["pending"] = slot.get("slot_status") != SLOT_RECEIVED
                pending_entry["abnormal"] = slot.get("slot_status") == SLOT_FAILED
                pending_entry["updated_at"] = _now_iso()

    def _occupy_slot_quota(
        self, source: dict[str, Any], group_no: str, confirmed: bool
    ) -> None:
        """数据源登记处按成组回执记录接入资格占用。

        同一组同一来源只记一次；confirmed=False 表示仅占用、资格待确认，
        凭证通过并实际接收后转为已确认。
        """
        occupied = json.loads(source.get("slot_occupancy", "[]"))
        if not any(item.get("group_no") == group_no for item in occupied):
            occupied.append({
                "group_no": group_no,
                "quota_status": "资格已确认" if confirmed else "资格占用待确认",
                "occupied_at": _now_iso(),
            })
            source["slot_occupancy"] = json.dumps(occupied, ensure_ascii=False)
            source["occupied_groups"] = str(len(occupied))
        elif confirmed:
            for item in occupied:
                if item.get("group_no") == group_no:
                    item["quota_status"] = "资格已确认"
                    item["confirmed_at"] = _now_iso()
            source["slot_occupancy"] = json.dumps(occupied, ensure_ascii=False)

    def _release_slot_quota(self, source: dict[str, Any], group_no: str) -> None:
        occupied = json.loads(source.get("slot_occupancy", "[]"))
        remaining = [item for item in occupied if item.get("group_no") != group_no]
        if len(remaining) != len(occupied):
            source["slot_occupancy"] = json.dumps(remaining, ensure_ascii=False)
            source["occupied_groups"] = str(len(remaining))

    def _ingest_one_source(
        self,
        group: dict[str, Any],
        slot: dict[str, Any],
        source: dict[str, Any],
        raw_logs: list[dict[str, Any]],
        time_window: str,
    ) -> dict[str, Any]:
        """为单个来源建立独立分批并落原始行（每条原始行只归自己的来源）。"""
        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source["来源标识"],
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs)),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source.get("source_version", 1),
            "group_no": group["成组编号"],
            "group_source": source["来源标识"],
            "started_at": _now_iso(),
        }
        store.rows("ingest_batch").append(batch_entry)

        raw_table = store.rows("raw_records")
        created_ids: list[int] = []
        for idx, log in enumerate(raw_logs):
            raw_id = _next_id("raw_records")
            record = {
                "id": raw_id,
                "批次编号": batch_no,
                "原始行号": str(idx + 1),
                "采集时间戳": _now_iso(),
                "原始正文": log.get("body", str(log)),
                "解析状态": "未解析",
                "status": "待解析",
                "pending": True,
                "abnormal": False,
                "batch_version": 1,
                "source_id": source["来源标识"],
                "group_no": group["成组编号"],
                "slot_id": str(slot["id"]),
                "ingested_at": _now_iso(),
            }
            raw_table.append(record)
            created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)

        slot.update({
            "slot_status": SLOT_RECEIVED,
            "status": SLOT_RECEIVED,
            "pending": False,
            "abnormal": False,
            "批次编号": batch_no,
            "record_count": len(created_ids),
            "raw_record_ids": json.dumps(created_ids),
            "原因": "",
            "received_at": _now_iso(),
        })
        self._occupy_slot_quota(source, group["成组编号"], confirmed=True)
        return batch_entry

    # ------------------------------------------------------------------
    # 对外能力
    # ------------------------------------------------------------------
    def submit_group(
        self,
        group_no: str,
        sources: list[dict[str, Any]],
        time_window: str,
    ) -> dict[str, Any]:
        """多选来源一次提交，逐条按实际来源分批接收，返回成组采集回执。

        sources: [{"source_id": "...", "raw_logs": [{"body": "..."} ...]}]
        重复提交同一 group_no 时走幂等续交逻辑，只认已建立的分批身份。
        """
        if not sources:
            return {"ok": False, "message": "至少选择一个数据源"}
        source_ids = [item.get("source_id", "") for item in sources]
        if any(not sid for sid in source_ids):
            return {"ok": False, "message": "存在空的来源标识"}
        if len(set(source_ids)) != len(source_ids):
            return {"ok": False, "message": "同一次提交内来源标识重复"}

        group = self._find_group(group_no)
        is_resubmit = group is not None
        if group is None:
            group = {
                "id": _next_id(GROUP_MODULE),
                "成组编号": group_no,
                "采集时间窗口": time_window,
                "来源总数": str(len(sources)),
                "已接收来源数": "0",
                "缺失来源": "[]",
                "组状态": GROUP_PARTIAL,
                "status": GROUP_PARTIAL,
                "pending": True,
                "abnormal": False,
                "正式记录可见": False,
                "created_at": _now_iso(),
                "提交次数": 0,
            }
            store.rows(GROUP_MODULE).append(group)
        group["提交次数"] = int(group.get("提交次数", 0)) + 1
        group["last_submit_at"] = _now_iso()

        receipts: list[dict[str, Any]] = []
        for item in sources:
            source_id = item["source_id"]
            raw_logs = item.get("raw_logs", [])
            source = self._find_source(source_id)

            # 未登记来源：标记该路故障，组内可定位到具体哪一批需要重做
            if source is None:
                slot = self._slot_of(group_no, source_id)
                if slot is None:
                    slot = {
                        "id": _next_id(SLOT_MODULE),
                        "成组编号": group_no,
                        "来源标识": source_id,
                        "批次编号": "",
                        "slot_status": SLOT_FAILED,
                        "status": SLOT_FAILED,
                        "pending": True,
                        "abnormal": True,
                        "record_count": 0,
                        "raw_record_ids": "[]",
                        "staged_logs": json.dumps(raw_logs, ensure_ascii=False),
                        "原因": f"数据源 {source_id} 未登记",
                        "created_at": _now_iso(),
                    }
                    store.rows(SLOT_MODULE).append(slot)
                receipts.append({
                    "source_id": source_id,
                    "slot_status": SLOT_FAILED,
                    "batch_no": "",
                    "record_count": 0,
                    "raw_record_ids": [],
                    "reason": slot["原因"],
                })
                continue

            existing = self._slot_of(group_no, source_id)
            if existing and existing.get("slot_status") == SLOT_RECEIVED:
                # 重复提交：只认已经建立的分批身份，不重复落原始行
                receipts.append({
                    "source_id": source_id,
                    "slot_status": SLOT_RECEIVED,
                    "batch_no": existing.get("批次编号", ""),
                    "record_count": existing.get("record_count", 0),
                    "raw_record_ids": json.loads(existing.get("raw_record_ids", "[]")),
                    "reason": "分批身份已建立，重复提交幂等跳过",
                    "idempotent": True,
                })
                continue

            # 凭证未就绪：先占用接入资格，原文暂存，等待补送
            if not self._credential_ready(source):
                self._occupy_slot_quota(source, group_no, confirmed=False)
                if existing is None:
                    existing = {
                        "id": _next_id(SLOT_MODULE),
                        "成组编号": group_no,
                        "来源标识": source_id,
                        "批次编号": "",
                        "slot_status": SLOT_PENDING,
                        "status": SLOT_PENDING,
                        "pending": True,
                        "abnormal": False,
                        "record_count": 0,
                        "raw_record_ids": "[]",
                        "staged_logs": "[]",
                        "原因": "凭证未就绪",
                        "created_at": _now_iso(),
                    }
                    store.rows(SLOT_MODULE).append(existing)
                existing["staged_logs"] = json.dumps(raw_logs, ensure_ascii=False)
                existing["slot_status"] = SLOT_PENDING
                existing["status"] = SLOT_PENDING
                existing["pending"] = True
                existing["原因"] = "凭证未就绪"
                receipts.append({
                    "source_id": source_id,
                    "slot_status": SLOT_PENDING,
                    "batch_no": "",
                    "record_count": 0,
                    "raw_record_ids": [],
                    "reason": "凭证未就绪，接入资格已占用，待凭证通过后补送",
                })
                continue

            # 凭证就绪：为该来源建立独立分批并接收原始行
            if existing is None:
                existing = {
                    "id": _next_id(SLOT_MODULE),
                    "成组编号": group_no,
                    "来源标识": source_id,
                    "批次编号": "",
                    "slot_status": SLOT_PENDING,
                    "status": SLOT_PENDING,
                    "pending": True,
                    "abnormal": False,
                    "record_count": 0,
                    "raw_record_ids": "[]",
                    "staged_logs": "[]",
                    "原因": "",
                    "created_at": _now_iso(),
                }
                store.rows(SLOT_MODULE).append(existing)
            # 补交时若未带新日志，沿用首次暂存的原文
            logs_to_ingest = raw_logs or json.loads(existing.get("staged_logs", "[]"))
            batch_entry = self._ingest_one_source(
                group, existing, source, logs_to_ingest, time_window
            )
            existing["staged_logs"] = "[]"
            receipts.append({
                "source_id": source_id,
                "slot_status": SLOT_RECEIVED,
                "batch_no": batch_entry["批次编号"],
                "record_count": int(batch_entry["记录总数"]),
                "raw_record_ids": json.loads(batch_entry["raw_record_ids"]),
                "reason": "",
            })

        self._refresh_group_state(group)
        return self._build_receipt(group, receipts, is_resubmit)

    def supplement_group(
        self,
        group_no: str,
        sources: list[dict[str, Any]],
        time_window: str | None = None,
    ) -> dict[str, Any]:
        """用原组编号补送尚缺来源；整组待补交部分在验证通过后由此继续完成。"""
        group = self._find_group(group_no)
        if group is None:
            return {"ok": False, "message": f"成组编号 {group_no} 不存在，无法补送"}
        window = time_window or group.get("采集时间窗口", "unknown")

        known = {s["来源标识"] for s in self._slots(group_no)}
        for item in sources:
            sid = item.get("source_id", "")
            if sid and sid not in known:
                return {
                    "ok": False,
                    "message": f"来源 {sid} 不属于成组 {group_no}，不能临时加塞",
                }
        return self.submit_group(group_no, sources, window)

    def get_group_receipt(self, group_no: str) -> dict[str, Any]:
        """查看第一次/最近一次成组回执，含每路分批、缺失来源、正式记录可见性。"""
        group = self._find_group(group_no)
        if group is None:
            return {"ok": False, "message": f"成组编号 {group_no} 不存在"}
        slots = self._slots(group_no)
        source_receipts = []
        for slot in slots:
            source_receipts.append({
                "source_id": slot["来源标识"],
                "slot_status": slot.get("slot_status"),
                "batch_no": slot.get("批次编号", ""),
                "record_count": slot.get("record_count", 0),
                "raw_record_ids": json.loads(slot.get("raw_record_ids", "[]")),
                "reason": slot.get("原因", ""),
            })
        return {
            "ok": True,
            "group": group,
            "group_no": group_no,
            "group_status": group.get("组状态"),
            "missing_sources": json.loads(group.get("缺失来源", "[]")),
            "formal_visible": bool(group.get("正式记录可见")),
            "source_receipts": source_receipts,
        }

    def get_formal_records(self, group_no: str) -> dict[str, Any]:
        """齐备后可见的正式记录；缺路期间明确拒绝并指出尚缺哪路。"""
        group = self._find_group(group_no)
        if group is None:
            return {"ok": False, "message": f"成组编号 {group_no} 不存在"}
        missing = json.loads(group.get("缺失来源", "[]"))
        if missing:
            return {
                "ok": False,
                "message": f"成组 {group_no} 尚未齐备，正式记录不可见",
                "missing_sources": missing,
                "formal_visible": False,
            }
        raw_rows = [
            r for r in store.rows("raw_records")
            if r.get("group_no") == group_no
        ]
        raw_rows.sort(key=lambda r: (str(r.get("source_id")), int(r.get("原始行号", 0))))
        return {
            "ok": True,
            "formal_visible": True,
            "group_no": group_no,
            "record_count": len(raw_rows),
            "records": raw_rows,
        }

    def get_partial_raw(self, group_no: str) -> dict[str, Any]:
        """缺一路时先利用已接收的原文：按来源返回已入库原始行与暂存原文。"""
        group = self._find_group(group_no)
        if group is None:
            return {"ok": False, "message": f"成组编号 {group_no} 不存在"}
        received_rows = [
            r for r in store.rows("raw_records")
            if r.get("group_no") == group_no
        ]
        staged: dict[str, Any] = {}
        for slot in self._slots(group_no):
            if slot.get("slot_status") == SLOT_PENDING:
                staged[slot["来源标识"]] = json.loads(slot.get("staged_logs", "[]"))
        return {
            "ok": True,
            "group_no": group_no,
            "missing_sources": json.loads(group.get("缺失来源", "[]")),
            "received_raw_records": received_rows,
            "staged_pending_logs": staged,
        }

    def list_pending(self, group_no: str | None = None) -> dict[str, Any]:
        """本组待处理清单：处理结果回写、关联每个源的原始行，定位需重做的批。"""
        rows = store.rows(PENDING_MODULE)
        if group_no:
            rows = [r for r in rows if r.get("成组编号") == group_no]
        return {"ok": True, "items": rows, "total": len(rows)}

    def retry_failed_slot(self, group_no: str, source_id: str) -> dict[str, Any]:
        """个别来源故障时，按来源定位到需要重做的那一批并重试。"""
        group = self._find_group(group_no)
        if group is None:
            return {"ok": False, "message": f"成组编号 {group_no} 不存在"}
        slot = self._slot_of(group_no, source_id)
        if slot is None:
            return {"ok": False, "message": f"来源 {source_id} 不在组 {group_no} 内"}
        if slot.get("slot_status") != SLOT_FAILED:
            return {"ok": False, "message": f"来源 {source_id} 当前状态 {slot.get('slot_status')}，无需重做"}
        source = self._find_source(source_id)
        if source is None:
            return {"ok": False, "message": f"数据源 {source_id} 仍未登记，无法重做"}
        if not self._credential_ready(source):
            slot["slot_status"] = SLOT_PENDING
            slot["status"] = SLOT_PENDING
            slot["原因"] = "凭证未就绪"
            self._refresh_group_state(group)
            return {"ok": True, "message": "故障源凭证仍未就绪，转待补交", "slot": slot}
        staged = json.loads(slot.get("staged_logs", "[]"))
        batch_entry = self._ingest_one_source(
            group, slot, source, staged, group.get("采集时间窗口", "unknown")
        )
        slot["staged_logs"] = "[]"
        self._refresh_group_state(group)
        return {
            "ok": True,
            "message": f"来源 {source_id} 分批重做完成",
            "batch_no": batch_entry["批次编号"],
        }

    def _build_receipt(
        self,
        group: dict[str, Any],
        receipts: list[dict[str, Any]],
        is_resubmit: bool,
    ) -> dict[str, Any]:
        group_no = group["成组编号"]
        missing = json.loads(group.get("缺失来源", "[]"))
        complete = not missing
        group["正式记录可见"] = complete
        return {
            "ok": True,
            "message": (
                f"成组 {group_no} 已齐备，正式记录可见"
                if complete
                else f"成组 {group_no} 部分接收，尚缺 {len(missing)} 路"
            ),
            "group_no": group_no,
            "group_status": group.get("组状态"),
            "is_resubmit": is_resubmit,
            "missing_sources": missing,
            "formal_visible": complete,
            "source_receipts": receipts,
            "pending_checklist": [
                {
                    "source_id": p.get("来源标识"),
                    "batch_no": p.get("分批编号"),
                    "status": p.get("处理状态"),
                    "raw_record_ids": json.loads(p.get("关联原始行", "[]")),
                    "reason": p.get("待补原因", ""),
                }
                for p in store.rows(PENDING_MODULE)
                if p.get("成组编号") == group_no
            ],
        }


group_ingest_service = GroupIngestService()
