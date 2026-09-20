"""端到端测试：覆盖证据链、许可合规、结构 ACL、分叉合并、阶段门与复现。"""

import json
import os
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from evidence.domain import DomainError, EvidenceService
from evidence.store import AppendOnlyStore
import service as service_module

H = "a" * 64
H2 = "b" * 64
H3 = "c" * 64

ACTOR1 = {"person_id": "u1", "party_id": "p1"}
ACTOR2 = {"person_id": "u2", "party_id": "p2"}


class EvidenceScenarioTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.clock = ["2026-02-01T00:00:00Z"]
        self.svc = self._service()

    def _service(self):
        store = AppendOnlyStore(self.tmp, time_func=lambda: self.clock[0])
        return EvidenceService(store)

    def _bootstrap(self):
        s = self.svc
        s.register_party({"party_id": "p1", "name": "本公司"})
        s.register_party({"party_id": "p2", "name": "合作方"})
        s.register_person({"person_id": "u1", "name": "甲", "party_id": "p1"})
        s.register_person({"person_id": "u2", "name": "乙", "party_id": "p2"})
        s.create_line({"line_id": "L1", "name": "疾病机制线",
                       "kind": "disease_mechanism"}, ACTOR1)
        s.create_line({"line_id": "L2", "name": "分子设计线",
                       "kind": "molecule_design", "parent_line_id": "L1",
                       "reason": "机制证据支持，分叉出设计竞争方案"}, ACTOR1)
        self.hyp = s.create_hypothesis(
            {"hypothesis_id": "H1", "disease": "纤维化",
             "mechanism": "受体X过度激活",
             "statement": "抑制受体X可下调通路Y"}, ACTOR1)
        self.data_stmt = s.register_data_statement(
            {"statement_id": "D1", "name": "合作方筛选库声明",
             "dataset_hash": H, "provenance": "p2 内部文库"}, ACTOR2)
        self.lic_v1 = s.grant_license(
            {"statement_id": "D1", "grantee_party_id": "p1", "version": "v1",
             "purposes": ["screening", "lead_optimization"],
             "valid_from": "2026-01-01T00:00:00Z",
             "valid_until": "2027-01-01T00:00:00Z",
             "confidential_until": "2031-01-01T00:00:00Z"}, ACTOR2)
        self.model = s.register_model(
            {"model_id": "M1", "name": "结合亲和力模型", "version": "3.2",
             "code_hash": H, "param_hash": H2, "training_data_hash": H,
             "training_statement_ids": ["D1"],
             "hyperparameters": {"lr": 0.001}}, ACTOR1)
        self.c1 = s.register_candidate(
            {"candidate_id": "C1", "structure": "CCO-NH-CO-c1ccccc1",
             "line_id": "L2", "name": "候选C1",
             "origin": {"hypothesis_id": "H1"}}, ACTOR1)
        self.c2 = s.register_candidate(
            {"candidate_id": "C2", "structure": "CCC-BR-CO-c1ccncc1",
             "line_id": "L2", "name": "候选C2"}, ACTOR1)
        self.run = s.record_model_run(
            {"run_id": "R1", "model_id": "M1", "line_id": "L2",
             "purpose": "screening",
             "ranking": [{"candidate_id": "C1", "score": 0.91},
                         {"candidate_id": "C2", "score": 0.33}]}, ACTOR1)
        s.attribute_candidate_design(
            {"candidate_id": "C1", "model_run_id": "R1"}, ACTOR1)
        self.batch = s.record_batch(
            {"batch_id": "B1", "candidate_id": "C1", "protocol_hash": H3},
            ACTOR1)
        self.meas = s.record_measurement(
            {"measurement_id": "E1", "batch_id": "B1", "assay": "IC50",
             "value": 12.0, "unit": "nM", "raw_payload_hash": H}, ACTOR1)

    # ---- 一次入账 ---------------------------------------------------- #

    def test_idempotent_retry_applied_once(self):
        self._bootstrap()
        event_a = self.svc.record_measurement(
            {"measurement_id": "E2", "batch_id": "B1", "assay": "溶解度",
             "value": 0.8, "unit": "mg/mL"}, ACTOR1, idem_key="retry-1")
        event_b = self.svc.record_measurement(
            {"measurement_id": "E2", "batch_id": "B1", "assay": "溶解度",
             "value": 0.8, "unit": "mg/mL"}, ACTOR1, idem_key="retry-1")
        self.assertEqual(event_a["event_id"], event_b["event_id"])
        with open(self.svc.store.path, encoding="utf-8") as f:
            lines = [l for l in f if l.strip()]
        self.assertEqual(len([l for l in lines if '"E2"' in l]), 1)

    def test_idempotency_conflict_rejected(self):
        self._bootstrap()
        kwargs = dict(actor=ACTOR1, idem_key="dup-key")
        self.svc.record_measurement(
            {"measurement_id": "E3", "batch_id": "B1", "assay": "A",
             "value": 1, "unit": "u"}, **kwargs)
        with self.assertRaises(Exception) as ctx:
            self.svc.record_measurement(
                {"measurement_id": "E4", "batch_id": "B1", "assay": "B",
                 "value": 2, "unit": "u"}, **kwargs)
        self.assertEqual(ctx.exception.code, "idempotency_conflict")

    def test_idempotency_survives_restart(self):
        self._bootstrap()
        ev1 = self.svc.record_batch(
            {"batch_id": "B2", "candidate_id": "C1", "protocol_hash": H3},
            ACTOR1, idem_key="restart-key")
        reloaded = self._service()
        ev2 = reloaded.record_batch(
            {"batch_id": "B2", "candidate_id": "C1", "protocol_hash": H3},
            ACTOR1, idem_key="restart-key")
        self.assertEqual(ev1["event_id"], ev2["event_id"])

    def test_concurrent_idempotent_upload_applied_once(self):
        import concurrent.futures
        self._bootstrap()
        body = {"batch_id": "BC", "candidate_id": "C1", "protocol_hash": H3}

        def upload(_):
            return self.svc.record_batch(body, ACTOR1, idem_key="concurrent-key")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(upload, range(8)))
        event_ids = {r["event_id"] for r in results}
        self.assertEqual(len(event_ids), 1)
        self.assertTrue(self.svc.store.verify_chain()["ok"])

    # ---- 原始测量只可追加更正 ---------------------------------------- #

    def test_measurement_correction_keeps_original(self):
        self._bootstrap()
        self.svc.correct_measurement(
            {"measurement_id": "E1", "correction_id": "X1", "new_value": 15.0,
             "reason": "复孔重算，原稀释倍数标注错误",
             "evidence_refs": [self.meas["event_id"]]}, ACTOR1)
        trace = self.svc.trace_candidate("C1", "p1")
        m = trace["measurements"][0]
        self.assertEqual(m["original"]["value"], 12.0)
        self.assertEqual(m["current_value"]["value"], 15.0)
        self.assertEqual(m["current_value"]["via_correction_id"], "X1")
        self.assertEqual(m["corrections"][0]["reason"],
                         "复孔重算，原稀释倍数标注错误")
        # 存储中原始事件原样存在。
        with open(self.svc.store.path, encoding="utf-8") as f:
            raw = f.read()
        self.assertIn('"measurement_id": "E1"', raw)
        self.assertIn('"value": 12.0', raw)

    def test_correction_chain_ordering(self):
        self._bootstrap()
        self.svc.correct_measurement(
            {"measurement_id": "E1", "correction_id": "X1", "new_value": 15.0,
             "reason": "第一次更正", "evidence_refs": [self.meas["event_id"]]},
            ACTOR1)
        self.svc.correct_measurement(
            {"measurement_id": "E1", "correction_id": "X2", "new_value": 14.2,
             "reason": "第二次更正", "prior_correction_id": "X1",
             "evidence_refs": [self.meas["event_id"]]}, ACTOR1)
        with self.assertRaises(DomainError):
            self.svc.correct_measurement(
                {"measurement_id": "E1", "correction_id": "X3", "new_value": 99,
                 "reason": "悬空前序", "prior_correction_id": "NOPE",
                 "evidence_refs": [self.meas["event_id"]]}, ACTOR1)
        trace = self.svc.trace_candidate("C1", "p1")
        self.assertEqual(trace["measurements"][0]["current_value"]["value"], 14.2)

    # ---- 许可收紧：阻止新计算，保留旧结论 ---------------------------- #

    def test_license_tightening_blocks_new_runs_but_keeps_history(self):
        self._bootstrap()
        # 合作方收紧许可：仅留存档用途，保密期不变。
        self.svc.grant_license(
            {"statement_id": "D1", "grantee_party_id": "p1", "version": "v2",
             "purposes": ["archival_only"],
             "valid_from": "2026-03-01T00:00:00Z",
             "valid_until": "2031-01-01T00:00:00Z"}, ACTOR2)
        self.clock[0] = "2026-03-02T00:00:00Z"
        with self.assertRaises(DomainError) as ctx:
            self.svc.record_model_run(
                {"run_id": "R2", "model_id": "M1", "line_id": "L2",
                 "purpose": "screening",
                 "ranking": [{"candidate_id": "C1", "score": 0.7}]}, ACTOR1)
        self.assertEqual(ctx.exception.code, "license_purpose_denied")
        self.assertEqual(ctx.exception.status, 403)
        # 当时合法的运行与结论仍然可查。
        trace = self.svc.trace_candidate("C1", "p1")
        self.assertEqual(trace["model_evidence"]["run_id"], "R1")
        self.assertEqual(
            trace["model_evidence"]["license_snapshot_at_run"]["D1"]["version"],
            "v1")
        stmt = trace["model_evidence"]["model"]["training_statements"][0]
        self.assertEqual(stmt["license_version_at_run"], "v1")
        self.assertEqual(stmt["current_license_version"], "v2")

    def test_license_expiry_blocks_run(self):
        self._bootstrap()
        self.clock[0] = "2028-01-01T00:00:00Z"
        with self.assertRaises(DomainError) as ctx:
            self.svc.record_model_run(
                {"run_id": "R3", "model_id": "M1",
                 "purpose": "screening",
                 "ranking": [{"candidate_id": "C1", "score": 1}]}, ACTOR1)
        self.assertEqual(ctx.exception.code, "license_expired")

    def test_only_data_owner_can_change_license(self):
        self._bootstrap()
        with self.assertRaises(DomainError) as ctx:
            self.svc.grant_license(
                {"statement_id": "D1", "grantee_party_id": "p1", "version": "vx",
                 "purposes": ["screening"],
                 "valid_from": "2026-01-01T00:00:00Z"}, ACTOR1)
        self.assertEqual(ctx.exception.status, 403)

    # ---- 结构保密边界 ------------------------------------------------ #

    def test_structure_acl_redaction(self):
        self._bootstrap()
        own = self.svc.trace_candidate("C1", "p1")
        self.assertEqual(own["candidate"]["structure"], "CCO-NH-CO-c1ccccc1")
        other = self.svc.trace_candidate("C1", "p2")
        self.assertEqual(other["candidate"]["structure"], "[REDACTED]")
        self.assertFalse(other["candidate"]["structure_visible"])
        # 显式授权后可见。
        self.svc.register_candidate(
            {"candidate_id": "C3", "structure": "SHARED-XYZ", "line_id": "L2",
             "visible_to_party_ids": ["p2"]}, ACTOR1)
        shared = self.svc.trace_candidate("C3", "p2")
        self.assertEqual(shared["candidate"]["structure"], "SHARED-XYZ")

    def test_gate_package_respects_structure_acl(self):
        self._bootstrap()
        gate = self._freeze_gate()
        pkg_p2 = self.svc.gate_package("G1", "p2")
        cand_materials = [m for m in pkg_p2["materials"]
                          if m["type"] == "candidate_registered"
                          and m["payload"]["candidate_id"] == "C1"]
        self.assertEqual(cand_materials[0]["payload"]["structure"], "[REDACTED]")
        pkg_p1 = self.svc.gate_package("G1", "p1")
        cand_p1 = [m for m in pkg_p1["materials"]
                   if m["type"] == "candidate_registered"
                   and m["payload"]["candidate_id"] == "C1"]
        self.assertEqual(cand_p1[0]["payload"]["structure"],
                         "CCO-NH-CO-c1ccccc1")

    # ---- 分叉与有证据合并 -------------------------------------------- #

    def test_merge_requires_evidence_and_preserves_fork(self):
        self._bootstrap()
        with self.assertRaises(DomainError) as ctx:
            self.svc.merge_lines(
                {"line_id": "L2", "into_line_id": "L1",
                 "evidence_event_ids": [], "justification": "想合就合"}, ACTOR1)
        self.assertEqual(ctx.exception.code, "evidence_required")
        with self.assertRaises(DomainError):
            self.svc.merge_lines(
                {"line_id": "L2", "into_line_id": "L1",
                 "evidence_event_ids": [self.batch["event_id"]],
                 "justification": "批次事件不是支持性证据"}, ACTOR1)
        self.svc.merge_lines(
            {"line_id": "L2", "into_line_id": "L1",
             "evidence_event_ids": [self.hyp["event_id"], self.run["event_id"]],
             "justification": "机制与设计两条线证据一致，合并推进"}, ACTOR1)
        # 源线仍保留，竞争方案不丢。
        self.assertIn("L2", self.svc.lines)
        self.assertEqual(self.svc.lines["L1"]["merged_from"][0]["line_id"], "L2")

    # ---- 人员判断 ---------------------------------------------------- #

    def test_judgments_continue_and_abandon_with_evidence(self):
        self._bootstrap()
        self.svc.record_judgment(
            {"judgment_id": "J1", "subject_type": "candidate", "subject_id": "C1",
             "decision": "continue", "rationale": "高分且IC50达标",
             "evidence_refs": [self.run["event_id"], self.meas["event_id"]]},
            ACTOR1)
        self.svc.record_judgment(
            {"judgment_id": "J2", "subject_type": "candidate", "subject_id": "C2",
             "decision": "abandon", "rationale": "模型得分过低，不再合成",
             "evidence_refs": [self.run["event_id"]]}, ACTOR1)
        with self.assertRaises(DomainError) as ctx:
            self.svc.record_judgment(
                {"judgment_id": "J3", "subject_type": "candidate",
                 "subject_id": "C1", "decision": "continue",
                 "rationale": "无证据判断", "evidence_refs": []}, ACTOR1)
        self.assertEqual(ctx.exception.code, "evidence_required")
        trace = self.svc.trace_candidate("C1", "p1")
        self.assertEqual([j["decision"] for j in trace["judgments"]], ["continue"])

    # ---- 阶段门冻结与复现 -------------------------------------------- #

    def _freeze_gate(self):
        return self.svc.freeze_stage_gate(
            {"gate_id": "G1", "stage": "preclinical", "line_id": "L2",
             "candidate_ids": ["C1"], "decision": "go",
             "summary": "同意进入临床前研究",
             "conflicts_of_interest": [
                 {"person_id": "u2", "declaration": "乙在C1系列化合物持有专利"}],
             "approvals": [
                 {"person_id": "u1", "vote": "approve",
                  "comment": "证据齐备"},
                 {"person_id": "u2", "vote": "abstain",
                  "comment": "利益冲突回避"}]}, ACTOR1)

    def test_stage_gate_freezes_materials_coi_approvals(self):
        self._bootstrap()
        self._freeze_gate()
        report = self.svc.verify_gate("G1")
        self.assertTrue(report["ok"], report["problems"])
        pkg = self.svc.gate_package("G1", "p1")
        types = {m["type"] for m in pkg["materials"]}
        self.assertIn("model_run_recorded", types)
        self.assertIn("model_registered", types)
        self.assertIn("data_statement_registered", types)
        self.assertIn("license_terms_granted", types)
        self.assertIn("measurement_recorded", types)
        self.assertEqual(pkg["conflicts_of_interest"][0]["declaration"],
                         "乙在C1系列化合物持有专利")
        self.assertEqual({a["vote"] for a in pkg["approvals"]},
                         {"approve", "abstain"})

    def test_gate_requires_approvals_and_coi_text(self):
        self._bootstrap()
        with self.assertRaises(DomainError) as ctx:
            self.svc.freeze_stage_gate(
                {"gate_id": "G1", "stage": "preclinical",
                 "candidate_ids": ["C1"], "decision": "go",
                 "approvals": []}, ACTOR1)
        self.assertEqual(ctx.exception.code, "approvals_required")
        with self.assertRaises(DomainError) as ctx:
            self.svc.freeze_stage_gate(
                {"gate_id": "G2", "stage": "preclinical",
                 "candidate_ids": ["C1"], "decision": "go",
                 "conflicts_of_interest": [{"person_id": "u1", "declaration": ""}],
                 "approvals": [{"person_id": "u1", "vote": "approve"}]}, ACTOR1)
        self.assertEqual(ctx.exception.code, "coi_declaration_required")

    def test_gate_remains_verifiable_after_later_appends(self):
        self._bootstrap()
        self._freeze_gate()
        # 冻结后追加新事件（新数据到达）不改变冻结材料。
        self.svc.correct_measurement(
            {"measurement_id": "E1", "correction_id": "X9", "new_value": 13.5,
             "reason": "门后补充复测",
             "evidence_refs": [self.meas["event_id"]]}, ACTOR1)
        self.assertTrue(self.svc.verify_gate("G1")["ok"])
        self.assertTrue(self.svc.store.verify_chain()["ok"])

    def test_trace_from_preclinical_candidate_reproduces_everything(self):
        self._bootstrap()
        self.svc.record_judgment(
            {"judgment_id": "J1", "subject_type": "candidate", "subject_id": "C1",
             "decision": "advance", "rationale": "进入临床前",
             "evidence_refs": [self.run["event_id"], self.meas["event_id"]]},
            ACTOR1)
        self._freeze_gate()
        trace = self.svc.trace_candidate("C1", "p1")
        self.assertTrue(trace["entered_preclinical"])
        self.assertEqual(trace["preclinical_gate_id"], "G1")
        # 数据、模型、反证/人工取舍齐全。
        self.assertEqual(trace["model_evidence"]["model"]["param_hash"], H2)
        self.assertEqual(trace["model_evidence"]["model"]["code_hash"], H)
        self.assertEqual(
            trace["model_evidence"]["model"]["training_statements"][0]
            ["dataset_hash"], H)
        self.assertEqual(len(trace["judgments"]), 1)
        self.assertEqual(trace["stage_gates"][0]["verify"]["ok"], True)

    def test_run_ranking_auto_links_candidate_without_attribution(self):
        # 候选先登记、后出现在运行排名中：即使没有显式归因事件，
        # 反查也必须能把模型/数据/许可证据连回候选。
        self._bootstrap()
        svc = self.svc
        svc.register_candidate(
            {"candidate_id": "C9", "structure": "C9-SMILES", "line_id": "L2"},
            ACTOR1)
        svc.record_model_run(
            {"run_id": "R9", "model_id": "M1", "line_id": "L2",
             "purpose": "screening",
             "ranking": [{"candidate_id": "C9", "score": 0.77}]}, ACTOR1)
        trace = svc.trace_candidate("C9", "p1")
        self.assertEqual(trace["model_evidence"]["run_id"], "R9")
        self.assertEqual(trace["model_evidence"]["model"]["param_hash"], H2)
        self.assertEqual(
            trace["model_evidence"]["model"]["training_statements"][0][
                "license_version_at_run"], "v1")
        self.assertFalse(trace["model_evidence"]["is_attributed_origin"])

    def test_run_precedes_candidate_registration(self):
        # 真实时序：算法数月筛出排名时候选尚未登记入库，之后再登记分子。
        self._bootstrap()
        svc = self.svc
        svc.record_model_run(
            {"run_id": "RE", "model_id": "M1", "line_id": "L2",
             "purpose": "screening",
             "ranking": [{"candidate_id": "CE", "score": 0.81}]}, ACTOR1)
        svc.register_candidate(
            {"candidate_id": "CE", "structure": "CE-SMILES", "line_id": "L2"},
            ACTOR1)
        trace = svc.trace_candidate("CE", "p1")
        self.assertEqual(trace["model_evidence"]["run_id"], "RE")
        self.assertEqual(trace["model_evidence"]["score"], 0.81)
        # 阶段门材料也应自动收齐运行/模型/数据/许可。
        svc.freeze_stage_gate(
            {"gate_id": "GE", "stage": "lead_optimization",
             "candidate_ids": ["CE"], "decision": "go",
             "approvals": [{"person_id": "u1", "vote": "approve"}]}, ACTOR1)
        types = {m["type"] for m in svc.gate_package("GE", "p1")["materials"]}
        self.assertIn("model_run_recorded", types)
        self.assertIn("license_terms_granted", types)

    # ---- 哈希链防篡改 ------------------------------------------------ #

    def test_tampering_is_detected(self):
        self._bootstrap()
        path = self.svc.store.path
        with open(path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        # 改写靶点假设事件中的历史内容。
        idx = next(i for i, l in enumerate(lines) if "纤维化" in l)
        self.assertIn("纤维化", lines[idx])
        lines[idx] = lines[idx].replace("纤维化", "被篡改的疾病")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        reopened = EvidenceService(
            AppendOnlyStore(self.tmp, time_func=lambda: self.clock[0]))
        self.assertFalse(reopened.store.verify_chain()["ok"])

    # ---- 基础校验 ---------------------------------------------------- #

    def test_hashes_and_references_validated(self):
        self._bootstrap()
        with self.assertRaises(DomainError):
            self.svc.register_model(
                {"model_id": "BAD", "name": "x", "version": "1",
                 "code_hash": "not-a-hash", "param_hash": H2,
                 "training_data_hash": H, "training_statement_ids": ["D1"]},
                ACTOR1)
        with self.assertRaises(DomainError) as ctx:
            self.svc.record_batch(
                {"batch_id": "BX", "candidate_id": "GHOST",
                 "protocol_hash": H3}, ACTOR1)
        self.assertEqual(ctx.exception.status, 404)

    def test_authentication_required(self):
        with self.assertRaises(DomainError) as ctx:
            self.svc.create_line(
                {"line_id": "LX", "name": "x", "kind": "molecule_design"},
                {"person_id": None, "party_id": None})
        self.assertEqual(ctx.exception.status, 401)


class HttpContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        service_module.reset_state(cls.tmp)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), service_module.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _req(self, method, path, body=None, headers=None, expect_error=False):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = Request(self.base + path, data=data, method=method,
                      headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urlopen(req, timeout=3) as resp:
                return resp.status, json.load(resp)
        except HTTPError as exc:
            return exc.code, json.load(exc)

    def test_health_contract_unchanged(self):
        status, body = self._req("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok", "service": "drug-research-evidence",
                                "name": "AI药研决策证据链"})

    def test_full_flow_over_http_with_idempotency_and_acl(self):
        s, _ = self._req("POST", "/v1/parties",
                         {"party_id": "hp1", "name": "内部"})
        self.assertEqual(s, 201)
        self._req("POST", "/v1/parties", {"party_id": "hp2", "name": "合作方"})
        self._req("POST", "/v1/persons",
                  {"person_id": "hu1", "name": "甲", "party_id": "hp1"})
        self._req("POST", "/v1/persons",
                  {"person_id": "hu2", "name": "乙", "party_id": "hp2"})
        h1 = {"X-User-Id": "hu1", "X-Party-Id": "hp1"}
        h2 = {"X-User-Id": "hu2", "X-Party-Id": "hp2"}

        self.assertEqual(self._req("POST", "/v1/lines",
                                   {"line_id": "HL1", "name": "线",
                                    "kind": "molecule_design"})[0], 401)
        self._req("POST", "/v1/lines",
                  {"line_id": "HL1", "name": "线",
                   "kind": "molecule_design"}, h1)
        # 幂等重试：同一键只入账一次，返回同一事件。
        body = {"candidate_id": "HC1", "structure": "SECRET", "line_id": "HL1"}
        a_status, a = self._req("POST", "/v1/candidates", body,
                                {**h1, "Idempotency-Key": "k1"})
        b_status, b = self._req("POST", "/v1/candidates", body,
                                {**h1, "Idempotency-Key": "k1"})
        self.assertEqual((a_status, b_status), (201, 201))
        self.assertEqual(a["event_id"], b["event_id"])
        # 同键不同体 → 409。
        conflict_status, _ = self._req(
            "POST", "/v1/candidates",
            {"candidate_id": "HC2", "structure": "OTHER", "line_id": "HL1"},
            {**h1, "Idempotency-Key": "k1"})
        self.assertEqual(conflict_status, 409)
        # 跨方反查脱敏。
        _, trace_other = self._req("GET", "/v1/candidates/HC1/trace", headers=h2)
        self.assertEqual(trace_other["candidate"]["structure"], "[REDACTED]")
        _, trace_own = self._req("GET", "/v1/candidates/HC1/trace", headers=h1)
        self.assertEqual(trace_own["candidate"]["structure"], "SECRET")
        # 链校验与 404。
        status, report = self._req("GET", "/v1/verify-chain")
        self.assertEqual(status, 200)
        self.assertTrue(report["ok"])
        self.assertEqual(self._req("GET", "/nope")[0], 404)


if __name__ == "__main__":
    unittest.main()
