"""成组多源批量采集行为测试。

覆盖：
- 多选来源一次提交、逐条按来源分批、成组回执；
- 数据源登记按回执记录接入资格占用/确认；
- 待处理清单回写并关联每个源的原始行；
- 个别来源故障可定位到需要重做的那一批；
- 同一组两路接入资格在不同时间确认，缺路时已接收原文可用、
  尚缺哪路与齐备后正式记录都可确认；
- 重复提交只认已建立的分批身份，原始行只归自己的来源；
- 整组待补交部分在验证通过后用原组编号续交完成；
- 厂区两个采集端带不同标识上送、一路先报凭证未就绪的端到端流程。
"""
from __future__ import annotations

import json

from app.store import store
from app.services.group_ingest import (
    GROUP_COMPLETE,
    GROUP_PARTIAL,
    SLOT_PENDING,
    SLOT_RECEIVED,
    SLOT_FAILED,
    group_ingest_service as svc,
)


def _register_validated(source_id: str) -> None:
    # 登记/验证复用既有流水线能力
    from app.services.pipeline import pipeline
    pipeline.register_source(source_id, "http", "factory")
    pipeline.validate_source(source_id)


def _register_only(source_id: str) -> None:
    from app.services.pipeline import pipeline
    pipeline.register_source(source_id, "http", "factory")


def _logs(prefix: str, n: int) -> list[dict]:
    return [{"body": f"{prefix}-line{i}|INFO|msg{i}"} for i in range(1, n + 1)]


class TestMultiSourceGroupSubmit:
    """多选来源一次提交，按实际来源分批接收。"""

    def test_group_submit_creates_one_batch_per_source(self):
        _register_validated("src-a")
        _register_validated("src-b")
        result = svc.submit_group(
            "GRP-0001",
            [
                {"source_id": "src-a", "raw_logs": _logs("a", 3)},
                {"source_id": "src-b", "raw_logs": _logs("b", 2)},
            ],
            "2026-11-21",
        )
        assert result["ok"] is True
        assert result["group_status"] == GROUP_COMPLETE
        assert result["formal_visible"] is True
        assert result["missing_sources"] == []

        receipts = {r["source_id"]: r for r in result["source_receipts"]}
        assert receipts["src-a"]["slot_status"] == SLOT_RECEIVED
        assert receipts["src-b"]["slot_status"] == SLOT_RECEIVED
        # 两个来源各自独立批号
        assert receipts["src-a"]["batch_no"] != receipts["src-b"]["batch_no"]
        assert receipts["src-a"]["record_count"] == 3
        assert receipts["src-b"]["record_count"] == 2

        # 采集批次表确实建立了两个不同来源的批次
        batches = [
            b for b in store.rows("ingest_batch")
            if b.get("group_no") == "GRP-0001"
        ]
        assert len(batches) == 2
        assert {b["所属数据源"] for b in batches} == {"src-a", "src-b"}

    def test_each_raw_record_belongs_to_its_own_source(self):
        _register_validated("src-a")
        _register_validated("src-b")
        svc.submit_group(
            "GRP-0002",
            [
                {"source_id": "src-a", "raw_logs": _logs("a", 2)},
                {"source_id": "src-b", "raw_logs": _logs("b", 4)},
            ],
            "2026-11-21",
        )
        raws = [r for r in store.rows("raw_records") if r.get("group_no") == "GRP-0002"]
        a_rows = [r for r in raws if r["source_id"] == "src-a"]
        b_rows = [r for r in raws if r["source_id"] == "src-b"]
        assert len(a_rows) == 2
        assert len(b_rows) == 4
        # 每条原始日志只归自己的来源，行号在各自来源内从 1 开始
        assert sorted(int(r["原始行号"]) for r in a_rows) == [1, 2]
        assert sorted(int(r["原始行号"]) for r in b_rows) == [1, 2, 3, 4]
        assert all("a-line" in r["原始正文"] for r in a_rows)
        assert all("b-line" in r["原始正文"] for r in b_rows)

    def test_empty_or_duplicate_sources_rejected(self):
        assert svc.submit_group("GRP-EMPTY", [], "w")["ok"] is False
        _register_validated("dup-x")
        bad = svc.submit_group(
            "GRP-DUP",
            [
                {"source_id": "dup-x", "raw_logs": []},
                {"source_id": "dup-x", "raw_logs": []},
            ],
            "w",
        )
        assert bad["ok"] is False
        assert "重复" in bad["message"]


class TestAccessQuota:
    """数据源登记根据成组回执记录各处占用的接入资格。"""

    def test_quota_occupied_then_confirmed(self):
        _register_validated("quota-ready")
        svc.submit_group(
            "GRP-Q1",
            [{"source_id": "quota-ready", "raw_logs": _logs("q", 1)}],
            "w",
        )
        source = next(s for s in store.rows("source_registry") if s["来源标识"] == "quota-ready")
        occupied = json.loads(source["slot_occupancy"])
        assert len(occupied) == 1
        assert occupied[0]["group_no"] == "GRP-Q1"
        assert occupied[0]["quota_status"] == "资格已确认"

    def test_quota_pending_when_credential_not_ready(self):
        _register_only("quota-pending-src")
        svc.submit_group(
            "GRP-Q2",
            [{"source_id": "quota-pending-src", "raw_logs": _logs("q", 1)}],
            "w",
        )
        source = next(s for s in store.rows("source_registry") if s["来源标识"] == "quota-pending-src")
        occupied = json.loads(source["slot_occupancy"])
        assert occupied[0]["quota_status"] == "资格占用待确认"

        # 凭证通过并补送后，同一占用记录转为已确认（不重复占用）
        from app.services.pipeline import pipeline
        pipeline.validate_source("quota-pending-src")
        svc.supplement_group(
            "GRP-Q2",
            [{"source_id": "quota-pending-src", "raw_logs": []}],
        )
        source = next(s for s in store.rows("source_registry") if s["来源标识"] == "quota-pending-src")
        occupied = json.loads(source["slot_occupancy"])
        assert len(occupied) == 1
        assert occupied[0]["quota_status"] == "资格已确认"


class TestPendingChecklist:
    """处理结果回写本组待处理清单并关联每个源的原始行。"""

    def test_checklist_links_raw_rows_per_source(self):
        _register_validated("chk-a")
        _register_validated("chk-b")
        svc.submit_group(
            "GRP-C1",
            [
                {"source_id": "chk-a", "raw_logs": _logs("a", 2)},
                {"source_id": "chk-b", "raw_logs": _logs("b", 3)},
            ],
            "w",
        )
        pending = svc.list_pending("GRP-C1")
        assert pending["total"] == 2
        by_source = {p["来源标识"]: p for p in pending["items"]}
        assert by_source["chk-a"]["处理状态"] == SLOT_RECEIVED
        assert len(json.loads(by_source["chk-a"]["关联原始行"])) == 2
        assert len(json.loads(by_source["chk-b"]["关联原始行"])) == 3
        # 关联的原始行确实属于各自来源
        raw_ids = json.loads(by_source["chk-b"]["关联原始行"])
        raws = {r["id"]: r for r in store.rows("raw_records")}
        assert all(raws[i]["source_id"] == "chk-b" for i in raw_ids)


class TestSourceFailure:
    """个别来源故障时可以定位需要再做哪一批。"""

    def test_unregistered_source_marked_failed_and_retryable(self):
        _register_validated("ok-src")
        result = svc.submit_group(
            "GRP-F1",
            [
                {"source_id": "ok-src", "raw_logs": _logs("ok", 2)},
                {"source_id": "ghost-src", "raw_logs": _logs("ghost", 1)},
            ],
            "w",
        )
        # 整组部分接收，故障路可定位
        assert result["group_status"] == GROUP_PARTIAL
        assert "ghost-src" in result["missing_sources"]
        ghost_receipt = next(r for r in result["source_receipts"] if r["source_id"] == "ghost-src")
        assert ghost_receipt["slot_status"] == SLOT_FAILED
        assert "未登记" in ghost_receipt["reason"]

        # 待处理清单中故障路标记异常，带得出要重做的是哪一路
        pending = svc.list_pending("GRP-F1")
        ghost_row = next(p for p in pending["items"] if p["来源标识"] == "ghost-src")
        assert ghost_row["abnormal"] is True
        assert ghost_row["处理状态"] == SLOT_FAILED

        # 故障源补登记并验证后重做该批
        _register_validated("ghost-src")
        retry = svc.retry_failed_slot("GRP-F1", "ghost-src")
        assert retry["ok"] is True
        receipt = svc.get_group_receipt("GRP-F1")
        assert receipt["group_status"] == GROUP_COMPLETE
        assert receipt["missing_sources"] == []

    def test_retry_unknown_slot_rejected(self):
        assert svc.retry_failed_slot("NO-GROUP", "x")["ok"] is False


class TestPartialGroupAndFormalRecords:
    """同一组两路资格不同时间确认：缺路先用原文，缺哪路与正式记录都可确认。"""

    def test_missing_source_partial_view(self):
        _register_validated("part-a")
        _register_only("part-b")  # 凭证未就绪
        first = svc.submit_group(
            "GRP-P1",
            [
                {"source_id": "part-a", "raw_logs": _logs("a", 3)},
                {"source_id": "part-b", "raw_logs": _logs("b", 2)},
            ],
            "2026-11-21",
        )
        # 第一次成组回执：一路已接收、一路待补交
        assert first["group_status"] == GROUP_PARTIAL
        assert first["missing_sources"] == ["part-b"]
        assert first["formal_visible"] is False

        receipt = svc.get_group_receipt("GRP-P1")
        slots = {r["source_id"]: r for r in receipt["source_receipts"]}
        assert slots["part-a"]["slot_status"] == SLOT_RECEIVED
        assert slots["part-b"]["slot_status"] == SLOT_PENDING
        assert "凭证未就绪" in slots["part-b"]["reason"]

        # 缺一路时先利用已接收原文
        partial = svc.get_partial_raw("GRP-P1")
        assert partial["missing_sources"] == ["part-b"]
        received = partial["received_raw_records"]
        assert len(received) == 3
        assert all(r["source_id"] == "part-a" for r in received)
        # 待补交来源的原文已暂存，不会丢
        assert partial["staged_pending_logs"]["part-b"] == _logs("b", 2)

        # 正式记录在齐备前不可见，并明确告知缺哪路
        formal_before = svc.get_formal_records("GRP-P1")
        assert formal_before["ok"] is False
        assert formal_before["missing_sources"] == ["part-b"]

        # 凭证通过后用原组编号补送
        from app.services.pipeline import pipeline
        pipeline.validate_source("part-b")
        second = svc.supplement_group(
            "GRP-P1",
            [{"source_id": "part-b", "raw_logs": []}],
        )
        assert second["ok"] is True
        assert second["group_status"] == GROUP_COMPLETE
        assert second["formal_visible"] is True

        # 齐备后正式记录可见，含两路全部原始行且归属正确
        formal = svc.get_formal_records("GRP-P1")
        assert formal["ok"] is True
        assert formal["record_count"] == 5
        by_source = {"part-a": 0, "part-b": 0}
        for rec in formal["records"]:
            by_source[rec["source_id"]] += 1
        assert by_source == {"part-a": 3, "part-b": 2}

    def test_supplement_unknown_group_rejected(self):
        assert svc.supplement_group("NOPE", [{"source_id": "x"}])["ok"] is False

    def test_supplement_cannot_inject_new_source(self):
        _register_validated("mem-a")
        svc.submit_group(
            "GRP-P2",
            [{"source_id": "mem-a", "raw_logs": _logs("a", 1)}],
            "w",
        )
        _register_validated("mem-intruder")
        result = svc.supplement_group(
            "GRP-P2",
            [{"source_id": "mem-intruder", "raw_logs": []}],
        )
        assert result["ok"] is False
        assert "不属于" in result["message"]


class TestIdempotentResubmit:
    """同一请求重复提交只认已建立的分批身份，原始行不重复、不串源。"""

    def test_resubmit_keeps_established_batch_identity(self):
        _register_validated("idem-a")
        _register_validated("idem-b")
        payload = [
            {"source_id": "idem-a", "raw_logs": _logs("a", 2)},
            {"source_id": "idem-b", "raw_logs": _logs("b", 1)},
        ]
        first = svc.submit_group("GRP-I1", payload, "w")
        first_batches = {
            r["source_id"]: r["batch_no"] for r in first["source_receipts"]
        }
        raw_count_after_first = len(
            [r for r in store.rows("raw_records") if r.get("group_no") == "GRP-I1"]
        )

        # 完全相同的请求再提交一次
        second = svc.submit_group("GRP-I1", payload, "w")
        assert second["is_resubmit"] is True
        second_batches = {
            r["source_id"]: r["batch_no"] for r in second["source_receipts"]
        }
        # 分批身份不变
        assert first_batches == second_batches
        assert all(r.get("idempotent") for r in second["source_receipts"])
        # 原始行没有重复落库
        raw_count_after_second = len(
            [r for r in store.rows("raw_records") if r.get("group_no") == "GRP-I1"]
        )
        assert raw_count_after_second == raw_count_after_first == 3

        # 每组每来源只有一个 slot、一个批次
        slots = [s for s in store.rows("group_slot") if s["成组编号"] == "GRP-I1"]
        assert len(slots) == 2
        batches = [b for b in store.rows("ingest_batch") if b.get("group_no") == "GRP-I1"]
        assert len(batches) == 2

    def test_resubmit_with_new_payload_does_not_duplicate_finished_source(self):
        _register_validated("idem-c")
        _register_only("idem-d")
        svc.submit_group(
            "GRP-I2",
            [
                {"source_id": "idem-c", "raw_logs": _logs("c", 2)},
                {"source_id": "idem-d", "raw_logs": _logs("d", 2)},
            ],
            "w",
        )
        # 重复提交时给已完成来源塞新日志，也必须被幂等忽略
        again = svc.submit_group(
            "GRP-I2",
            [
                {"source_id": "idem-c", "raw_logs": _logs("c-new", 9)},
                {"source_id": "idem-d", "raw_logs": _logs("d", 2)},
            ],
            "w",
        )
        c_receipt = next(r for r in again["source_receipts"] if r["source_id"] == "idem-c")
        assert c_receipt["idempotent"] is True
        c_rows = [
            r for r in store.rows("raw_records")
            if r.get("group_no") == "GRP-I2" and r["source_id"] == "idem-c"
        ]
        assert len(c_rows) == 2
        assert all("c-line" in r["原始正文"] for r in c_rows)


class TestFactoryTwoCollectorsEndToEnd:
    """厂区两个采集端带不同标识上送，一路先报凭证未就绪的完整流程。"""

    def test_full_factory_workflow(self):
        from app.services.pipeline import pipeline

        # 厂区两个采集端对应的数据源：A 端凭证就绪，B 端凭证尚未就绪
        pipeline.register_source("plant-terminal-A", "mqtt", "plant-east")
        pipeline.validate_source("plant-terminal-A")
        pipeline.register_source("plant-terminal-B", "mqtt", "plant-east")

        group_no = "GRP-PLANT-20261121"
        # 第一次成组提交：两端各自带标识上送
        first = svc.submit_group(
            group_no,
            [
                {"source_id": "plant-terminal-A", "raw_logs": [
                    {"body": "2026-11-21T08:00|INFO|A-boiler-online"},
                    {"body": "2026-11-21T08:01|WARN|A-vibration"},
                ]},
                {"source_id": "plant-terminal-B", "raw_logs": [
                    {"body": "2026-11-21T08:00|INFO|B-conveyor-online"},
                ]},
            ],
            "2026-11-21T08:00",
        )

        # 第一次成组回执：A 已接收，B 凭证未就绪待补交
        assert first["ok"] is True
        first_slots = {r["source_id"]: r for r in first["source_receipts"]}
        assert first_slots["plant-terminal-A"]["slot_status"] == SLOT_RECEIVED
        assert first_slots["plant-terminal-B"]["slot_status"] == SLOT_PENDING
        assert first["missing_sources"] == ["plant-terminal-B"]
        batch_a = first_slots["plant-terminal-A"]["batch_no"]
        assert batch_a.startswith("INGE-")

        # 接入资格：A 已确认、B 仅占用待确认
        src_a = next(s for s in store.rows("source_registry") if s["来源标识"] == "plant-terminal-A")
        src_b = next(s for s in store.rows("source_registry") if s["来源标识"] == "plant-terminal-B")
        quota_a = json.loads(src_a["slot_occupancy"])[0]
        quota_b = json.loads(src_b["slot_occupancy"])[0]
        assert quota_a["group_no"] == group_no and quota_a["quota_status"] == "资格已确认"
        assert quota_b["group_no"] == group_no and quota_b["quota_status"] == "资格占用待确认"

        # 缺路期间 A 端原文已可利用，B 端原文暂存
        partial = svc.get_partial_raw(group_no)
        assert len(partial["received_raw_records"]) == 2
        assert partial["staged_pending_logs"]["plant-terminal-B"][0]["body"].endswith("B-conveyor-online")

        # B 端凭证通过，用原组编号补送（不带新日志，沿用首次暂存原文）
        pipeline.validate_source("plant-terminal-B")
        second = svc.supplement_group(
            group_no,
            [{"source_id": "plant-terminal-B", "raw_logs": []}],
        )
        assert second["group_status"] == GROUP_COMPLETE
        second_slots = {r["source_id"]: r for r in second["source_receipts"]}
        batch_b = second_slots["plant-terminal-B"]["batch_no"]
        assert batch_b.startswith("INGE-")
        assert batch_b != batch_a  # 两路批号不同

        # 核对各批号记录的原始行
        raws = [r for r in store.rows("raw_records") if r.get("group_no") == group_no]
        rows_a = [r for r in raws if r["批次编号"] == batch_a]
        rows_b = [r for r in raws if r["批次编号"] == batch_b]
        assert len(rows_a) == 2 and all(r["source_id"] == "plant-terminal-A" for r in rows_a)
        assert len(rows_b) == 1 and all(r["source_id"] == "plant-terminal-B" for r in rows_b)
        assert rows_b[0]["原始正文"].endswith("B-conveyor-online")
        # 行号在各自批次内独立计数
        assert [r["原始行号"] for r in rows_a] == ["1", "2"]
        assert [r["原始行号"] for r in rows_b] == ["1"]

        # 正式记录齐备可见，待处理清单全部关闭
        formal = svc.get_formal_records(group_no)
        assert formal["ok"] is True and formal["record_count"] == 3
        pending = svc.list_pending(group_no)
        assert all(p["处理状态"] == SLOT_RECEIVED for p in pending["items"])
        assert all(not p["pending"] for p in pending["items"])

        # B 端接入资格此时也已确认，且仍只占用一条
        src_b = next(s for s in store.rows("source_registry") if s["来源标识"] == "plant-terminal-B")
        occupied_b = json.loads(src_b["slot_occupancy"])
        assert len(occupied_b) == 1
        assert occupied_b[0]["quota_status"] == "资格已确认"

    def test_repeated_same_request_returns_same_receipt_identity(self):
        from app.services.pipeline import pipeline
        pipeline.register_source("rep-a", "http", "plant")
        pipeline.validate_source("rep-a")
        group_no = "GRP-REPEAT"
        r1 = svc.submit_group(
            group_no, [{"source_id": "rep-a", "raw_logs": _logs("r", 2)}], "w"
        )
        r2 = svc.submit_group(
            group_no, [{"source_id": "rep-a", "raw_logs": _logs("r", 2)}], "w"
        )
        assert r1["source_receipts"][0]["batch_no"] == r2["source_receipts"][0]["batch_no"]
        assert r2["source_receipts"][0]["idempotent"] is True
        # 再查回执，身份保持一致
        receipt = svc.get_group_receipt(group_no)
        assert receipt["source_receipts"][0]["batch_no"] == r1["source_receipts"][0]["batch_no"]
