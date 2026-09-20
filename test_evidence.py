"""逐条验证研发负责人提出的决策证据服务需求。"""

import json
import unittest
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

from evidence import (
    EvidenceError,
    EvidenceStore,
    PolicyViolation,
    UnknownEntity,
)

T0 = "2026-03-01T00:00:00+00:00"
T1 = "2026-06-01T00:00:00+00:00"
T2 = "2026-09-01T00:00:00+00:00"


def build_lineage(store):
    """搭一条完整研发链：假设→数据→训练→筛选→分子→批次→实验→判断。"""
    hyp = store.ingest(
        "target_hypothesis",
        {"target": "KINASE-X", "statement": "抑制 KINASE-X 可阻断肿瘤增殖"},
        actor="biologist")[0]
    decl = store.ingest(
        "data_declaration", {"dataset": "ChEMBL-kinase", "rows": 15230},
        actor="partner:A", compartment="partner:A")[0]
    lic = store.register_license(
        subject=decl.id, purposes=["training", "screening"],
        compartments=["internal"], not_after="2027-01-01T00:00:00+00:00",
        actor="counsel")[0]
    train, model_out, _ = store.derive(
        kind="training", inputs=[decl.id], purpose="training",
        actor="algo", compartment="internal",
        outputs=[{"type": "model_version",
                  "payload": {"name": "ranknet", "version": "v3"}}],
        params_hash="sha256:params-v3",
        extra_refs=(("tests_hypothesis", hyp.id),))
    screen, mol_out, _ = store.derive(
        kind="screening", inputs=[model_out[0]], purpose="screening",
        actor="algo", compartment="internal", model=model_out[0],
        params_hash="sha256:params-v3",
        outputs=[{"type": "candidate_molecule",
                  "payload": {"name": "MOL-101", "structure": "c1ccccc1"},
                  "compartment": "partner:A"}],
        extra_refs=(("tests_hypothesis", hyp.id),))
    batch = store.ingest(
        "synthesis_batch", {"batch_no": "B-2026-014"},
        refs=[("synthesized", mol_out[0])],
        actor="partner:A", compartment="partner:A")[0]
    result = store.ingest(
        "experiment_result", {"assay": "IC50", "value_nm": 42},
        refs=[("measured_on", batch.id)], actor="wetlab", compartment="shared")[0]
    counter = store.ingest(
        "experiment_result", {"assay": "IC50", "value_nm": None,
                              "note": "独立复测未重现"},
        refs=[("contradicts", hyp.id)], actor="wetlab", compartment="shared")[0]
    judgment = store.ingest(
        "judgment",
        {"decision": "continue", "rationale": "单次复测失败但量效趋势支持，加做正交实验"},
        refs=[("regards", mol_out[0])], actor="project-lead")[0]
    return SimpleNamespace(
        hyp=hyp.id, decl=decl.id, lic=lic.id, train=train.id, screen=screen.id,
        model=model_out[0], mol=mol_out[0], batch=batch.id,
        result=result.id, counter=counter.id, judgment=judgment.id,
    )


class EntityGraphTest(unittest.TestCase):
    """需求：各类事实彼此引用，且引用必须指向已入账实体。"""

    def test_full_lineage_references_each_other(self):
        store = EvidenceStore()
        ids = build_lineage(store)
        graph = store.traceback(ids.mol)
        types = {e["type"] for e in graph["entities"]}
        self.assertTrue({
            "target_hypothesis", "data_declaration", "model_version",
            "candidate_molecule", "synthesis_batch", "experiment_result",
            "judgment", "derivation",
        } <= types)

    def test_dangling_reference_is_rejected(self):
        store = EvidenceStore()
        with self.assertRaises(UnknownEntity):
            store.ingest("judgment", {"decision": "drop"},
                         refs=[("regards", "0" * 32)], actor="lead")


class AppendOnlyCorrectionTest(unittest.TestCase):
    """需求：原始测量只能追加更正，不能覆盖。"""

    def test_correction_appends_and_preserves_original(self):
        store = EvidenceStore()
        result = store.ingest("experiment_result", {"value": 10}, actor="wetlab")[0]
        corrected = store.correct(result.id, {"value": 12},
                                  actor="wetlab", reason="单位换算错误")
        self.assertEqual(store.get(result.id).payload, {"value": 10})
        self.assertEqual(store.latest(result.id).id, corrected.id)
        self.assertEqual([e.id for e in store.history(result.id)],
                         [result.id, corrected.id])
        self.assertEqual(corrected.corrects, result.id)
        self.assertEqual(corrected.note, "单位换算错误")

    def test_entity_is_structurally_immutable(self):
        store = EvidenceStore()
        result = store.ingest("experiment_result", {"value": 10}, actor="wetlab")[0]
        with self.assertRaises(FrozenInstanceError):
            result.payload = {"value": 99}
        self.assertEqual(store.get(result.id).payload, {"value": 10})


class IdempotentIngestTest(unittest.TestCase):
    """需求：合作方上传重试保持一次入账。"""

    def test_retry_with_same_key_is_recorded_once(self):
        store = EvidenceStore()
        before = len(store._entities)
        first, created1 = store.ingest(
            "data_declaration", {"dataset": "D1"},
            actor="partner:A", idempotency_key="upload-7")
        second, created2 = store.ingest(
            "data_declaration", {"dataset": "D1"},
            actor="partner:A", idempotency_key="upload-7")
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertEqual(first.id, second.id)
        self.assertEqual(len(store._entities), before + 1)

    def test_idempotency_key_is_scoped_per_uploader(self):
        store = EvidenceStore()
        a, _ = store.ingest("data_declaration", {"dataset": "D1"},
                            actor="partner:A", idempotency_key="upload-7")
        b, created = store.ingest("data_declaration", {"dataset": "D1"},
                                  actor="partner:B", idempotency_key="upload-7")
        self.assertTrue(created)
        self.assertNotEqual(a.id, b.id)

    def test_derivation_retry_does_not_duplicate_outputs(self):
        store = EvidenceStore()
        before = len(store._entities)
        d1, out1, created1 = store.derive(
            kind="screening", inputs=[], purpose="screening",
            actor="algo", compartment="internal",
            outputs=[{"type": "candidate_molecule", "payload": {"name": "M1"}}],
            idempotency_key="run-3")
        d2, out2, created2 = store.derive(
            kind="screening", inputs=[], purpose="screening",
            actor="algo", compartment="internal",
            outputs=[{"type": "candidate_molecule", "payload": {"name": "M1"}}],
            idempotency_key="run-3")
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertEqual((d1.id, out1), (d2.id, out2))
        self.assertEqual(len(store._entities), before + 2)


class LicenseEnforcementTest(unittest.TestCase):
    """需求：许可/保密期变化阻断新的不合规计算，但保留当时合法的结论。"""

    def setUp(self):
        self.store = EvidenceStore()
        self.decl = self.store.ingest(
            "data_declaration", {"dataset": "D1"}, actor="partner:A")[0]
        self.lic = self.store.register_license(
            subject=self.decl.id, purposes=["training"],
            compartments=["internal"], not_after=T1, actor="counsel")[0]

    def test_purpose_not_granted_is_blocked(self):
        with self.assertRaises(PolicyViolation):
            self.store.derive(kind="screening", inputs=[self.decl.id],
                              purpose="screening", actor="algo",
                              compartment="internal", at=T0)

    def test_confidentiality_window_expiry_blocks_new_computation(self):
        self.store.derive(kind="training", inputs=[self.decl.id],
                          purpose="training", actor="algo",
                          compartment="internal", at=T0)
        with self.assertRaises(PolicyViolation):
            self.store.derive(kind="training", inputs=[self.decl.id],
                              purpose="training", actor="algo",
                              compartment="internal", at=T2)

    def test_policy_rejection_leaves_no_trace(self):
        before = len(self.store._entities)
        with self.assertRaises(PolicyViolation):
            self.store.derive(kind="x", inputs=[self.decl.id], purpose="screening",
                              actor="algo", compartment="internal", at=T0)
        self.assertEqual(len(self.store._entities), before)

    def test_amendment_blocks_new_but_preserves_prior_conclusion(self):
        derivation, _, _ = self.store.derive(
            kind="training", inputs=[self.decl.id], purpose="training",
            actor="algo", compartment="internal", at=T0)
        amended = self.store.amend_license(
            self.lic.id, purposes=[], actor="counsel",
            reason="合作方撤回训练授权", at=T1)
        with self.assertRaises(PolicyViolation):
            self.store.derive(kind="training", inputs=[self.decl.id],
                              purpose="training", actor="algo",
                              compartment="internal", at=T1)
        # 当时合法形成的结论仍在，且其法律依据快照指向许可的原始版本。
        basis = derivation.payload["legal_basis"]
        self.assertEqual(basis[0]["license"], self.lic.id)
        self.assertEqual(basis[0]["hash"], self.lic.hash)
        self.assertEqual(self.store.get(self.lic.id).payload["purposes"], ["training"])
        self.assertEqual([e.id for e in self.store.history(self.lic.id)],
                         [self.lic.id, amended.id])

    def test_license_obligation_flows_downstream(self):
        """用受约束数据训出的模型，继续受同一许可约束。"""
        _, model_out, _ = self.store.derive(
            kind="training", inputs=[self.decl.id], purpose="training",
            actor="algo", compartment="internal", at=T0,
            outputs=[{"type": "model_version", "payload": {"name": "m"}}])
        self.store.amend_license(self.lic.id, purposes=["training"],
                                 not_after=T1, actor="counsel",
                                 reason="保密期收紧", at=T1)
        with self.assertRaises(PolicyViolation):
            self.store.derive(kind="screening", inputs=[model_out[0]],
                              purpose="screening", actor="algo",
                              compartment="internal", at=T0)


class ExplorationLineTest(unittest.TestCase):
    """需求：两条探索线可分叉保留竞争方案，也可凭证据合并。"""

    def test_fork_keeps_competing_lines_apart(self):
        store = EvidenceStore()
        trunk = store.ingest("target_hypothesis", {"target": "X"}, actor="bio")[0]
        store.fork("mechanism", from_branch="main",
                   rationale="疾病机制线", actor="lead")
        store.fork("design", from_branch="main",
                   rationale="分子设计线", actor="lead")
        m1 = store.ingest("experiment_result", {"pathway": "p1"},
                          branch="mechanism", actor="bio")[0]
        d1 = store.ingest("candidate_molecule", {"name": "M1"},
                          branch="design", actor="algo")[0]
        mech_view = store.branch_view("mechanism")
        design_view = store.branch_view("design")
        self.assertIn(trunk.id, mech_view)
        self.assertIn(trunk.id, design_view)
        self.assertIn(m1.id, mech_view)
        self.assertNotIn(m1.id, design_view)
        self.assertIn(d1.id, design_view)
        self.assertNotIn(d1.id, mech_view)

    def test_merge_unites_lines_on_evidence_and_keeps_source(self):
        store = EvidenceStore()
        store.fork("mechanism", from_branch="main", rationale="机制线", actor="lead")
        store.fork("design", from_branch="main", rationale="设计线", actor="lead")
        evidence = store.ingest("experiment_result", {"pathway": "p1"},
                                branch="design", actor="bio")[0]
        merge = store.merge("mechanism", source="design",
                            evidence=[evidence.id],
                            rationale="实验证实两条线指向同一机制", actor="lead")
        self.assertIn(evidence.id, store.branch_view("mechanism"))
        # 合并记录引用支撑证据；来源线保留，竞争方案仍可查。
        self.assertIn(("evidence", evidence.id),
                      [(r, t) for r, t in merge.refs])
        self.assertIn(evidence.id, store.branch_view("design"))

    def test_entities_after_fork_do_not_leak_into_sibling(self):
        store = EvidenceStore()
        store.fork("design", from_branch="main", rationale="设计线", actor="lead")
        late = store.ingest("experiment_result", {"late": True}, actor="bio")[0]
        self.assertNotIn(late.id, store.branch_view("design"))


class StageGateTest(unittest.TestCase):
    """需求：阶段门冻结所见材料、利益冲突与批准意见。"""

    def test_freeze_pins_exact_versions_seen_by_committee(self):
        store = EvidenceStore()
        ids = build_lineage(store)
        gate = store.freeze_stage_gate(
            name="PCC 评审", branch="main",
            packet=[ids.mol, ids.result],
            coi=[{"person": "Dr.W", "disclosure": "持有合作方A期权，已回避投票"}],
            approvals=[{"person": "Dr.W", "decision": "approve",
                        "rationale": "接受继续推进，附正交实验条件"}],
            actor="chair")
        self.assertTrue(store.verify_stage_gate(gate.id))
        # 会后原始测量被追加更正：冻结清单仍钉在委员会所见的版本上。
        store.correct(ids.result, {"assay": "IC50", "value_nm": 55},
                      actor="wetlab", reason="复测均值")
        self.assertTrue(store.verify_stage_gate(gate.id))
        pinned = {e["entity"]: e["hash"] for e in gate.payload["manifest"]}
        self.assertEqual(pinned[ids.result], store.get(ids.result).hash)
        self.assertEqual(store.get(ids.result).payload["value_nm"], 42)
        self.assertEqual(store.latest(ids.result).payload["value_nm"], 55)
        self.assertEqual(gate.payload["coi"][0]["person"], "Dr.W")
        self.assertEqual(gate.payload["approvals"][0]["decision"], "approve")


class TracebackTest(unittest.TestCase):
    """需求：从候选物反查，完整重现数据、模型、反证与人工取舍。"""

    def test_traceback_recovers_everything_used_and_weighed(self):
        store = EvidenceStore()
        ids = build_lineage(store)
        graph = store.traceback(ids.mol)
        found = {e["id"] for e in graph["entities"]}
        for entity_id in (ids.hyp, ids.decl, ids.model, ids.mol, ids.batch,
                          ids.result, ids.counter, ids.judgment,
                          ids.train, ids.screen):
            self.assertIn(entity_id, found)
        relations = {e[1] for e in graph["edges"]}
        for rel in ("consumes", "produces", "uses_model", "measured_on",
                    "contradicts", "regards", "tests_hypothesis"):
            self.assertIn(rel, relations)
        # 反证与人工取舍的理由都在图里。
        payloads = [e.get("payload", {}) for e in graph["entities"]]
        self.assertTrue(any(p.get("note") == "独立复测未重现" for p in payloads))
        self.assertTrue(any(p.get("decision") == "continue" for p in payloads))

    def test_traceback_includes_correction_chain(self):
        store = EvidenceStore()
        result = store.ingest("experiment_result", {"value": 10}, actor="wetlab")[0]
        corrected = store.correct(result.id, {"value": 12},
                                  actor="wetlab", reason="单位错误")
        graph = store.traceback(result.id)
        found = {e["id"] for e in graph["entities"]}
        self.assertEqual(found, {result.id, corrected.id})


class ConfidentialityBoundaryTest(unittest.TestCase):
    """需求：不向无权合作方泄露另一方的化合物结构。"""

    def test_partner_view_redacts_other_partners_structure(self):
        store = EvidenceStore()
        ids = build_lineage(store)
        graph = store.traceback(ids.mol, viewer={"shared", "partner:B"})
        serialized = json.dumps(graph, ensure_ascii=False)
        self.assertNotIn("c1ccccc1", serialized)
        self.assertNotIn("ChEMBL-kinase", serialized)
        by_id = {e["id"]: e for e in graph["entities"]}
        self.assertTrue(by_id[ids.mol]["redacted"])
        self.assertNotIn("payload", by_id[ids.mol])
        # 共享的实验结论对合作方B仍然可见。
        self.assertEqual(by_id[ids.result]["payload"]["value_nm"], 42)

    def test_internal_view_sees_everything(self):
        store = EvidenceStore()
        ids = build_lineage(store)
        graph = store.traceback(ids.mol)
        self.assertIn("c1ccccc1", json.dumps(graph, ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
