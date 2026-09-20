"""领域规则：把追加事件投影为可查询的研发证据状态。

设计要点：
- 所有写操作都转换为一条追加事件；投影只随事件追加而改变，从不删除历史。
- 原始测量不可修改，"更正"是追加 correction 事件；读数取更正链末端，原值保留。
- 模型运行（新计算）发生时按*当前*许可版本校验用途与有效期；许可收紧只阻断
  之后的运行，不删除当时合法形成的运行、判断与门结论。
- 化合物结构按属主 + 显式授权名单做读取脱敏，探索线合并不自动扩大结构授权。
- 阶段门冻结捕获事件清单（含各自哈希）、冻结时刻链头、利益冲突与批准意见。
"""

from __future__ import annotations

import functools
from datetime import datetime, timezone
from typing import Optional

from .store import AppendOnlyStore, StoreError

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DECISIONS = {"continue", "abandon", "hold", "advance"}
GATE_DECISIONS = {"go", "no_go", "hold"}
LINE_KINDS = {"disease_mechanism", "molecule_design"}
EVIDENCE_BEARING_TYPES = {
    "target_hypothesis_created",
    "model_run_recorded",
    "measurement_recorded",
    "measurement_corrected",
    "judgment_recorded",
}


class DomainError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status


def parse_ts(value: str) -> datetime:
    return datetime.strptime(value, TS_FORMAT).replace(tzinfo=timezone.utc)


def command(fn):
    """写命令装饰器：在业务校验*之前*处理幂等重试，并串行化写命令。

    这样"同一上传请求的重放"永远返回原事件，不会撞上实体唯一性校验；
    同一幂等键携带不同请求体则得到 409。
    """

    @functools.wraps(fn)
    def wrapper(self, payload, actor=None, idem_key=None):
        with self.store.write_lock:
            pending = None
            if idem_key is not None:
                digest = self.store.request_digest(payload, actor or {})
                hit = self.store.peek_idempotent(idem_key)
                if hit is not None:
                    event, stored_digest = hit
                    if stored_digest == digest:
                        return event
                    raise StoreError(
                        "idempotency_conflict",
                        "幂等键已用于另一请求，不得重复入账",
                        status=409,
                    )
                pending = (idem_key, digest)
            self._idem_digests.append(pending)
            try:
                return fn(self, payload, actor=actor, idem_key=idem_key)
            finally:
                self._idem_digests.pop()

    return wrapper


def is_sha256_hex(value) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(c in "0123456789abcdef" for c in value.lower())
    )


class EvidenceService:
    def __init__(self, store: AppendOnlyStore):
        self.store = store
        # 索引
        self.events_by_id: dict[str, dict] = {}
        self.parties: dict[str, dict] = {}
        self.persons: dict[str, dict] = {}
        self.lines: dict[str, dict] = {}
        self.hypotheses: dict[str, dict] = {}
        self.statements: dict[str, dict] = {}
        # (statement_id, grantee_party_id) -> 按时间追加的版本列表，末端为当前版
        self.licenses: dict[tuple[str, str], list[dict]] = {}
        self.models: dict[str, dict] = {}
        self.runs: dict[str, dict] = {}
        self.candidates: dict[str, dict] = {}
        self.design_attributions: list[dict] = []
        self.batches: dict[str, dict] = {}
        self.measurements: dict[str, dict] = {}
        self.corrections: list[dict] = []
        self.judgments: list[dict] = []
        self.gates: dict[str, dict] = {}
        # 命令装饰器压入的幂等摘要栈（写命令在 write_lock 内串行执行）。
        self._idem_digests: list[Optional[str]] = []
        self.store.replay(self._apply)

    # ================================================================ #
    # 投影
    # ================================================================ #

    def _apply(self, event: dict) -> None:
        p = event["payload"]
        t = event["type"]
        self.events_by_id[event["event_id"]] = event

        if t == "party_registered":
            self.parties[p["party_id"]] = {"party_id": p["party_id"], "name": p["name"]}
        elif t == "person_registered":
            self.persons[p["person_id"]] = {
                "person_id": p["person_id"],
                "name": p["name"],
                "party_id": p["party_id"],
            }
        elif t == "exploration_line_created":
            self.lines[p["line_id"]] = {
                "line_id": p["line_id"],
                "name": p["name"],
                "kind": p["kind"],
                "parent_line_id": p.get("parent_line_id"),
                "merged_from": [],
            }
        elif t == "exploration_lines_merged":
            into = self.lines[p["into_line_id"]]
            into["merged_from"].append(
                {
                    "line_id": p["line_id"],
                    "evidence_event_ids": p["evidence_event_ids"],
                    "justification": p["justification"],
                    "event_id": event["event_id"],
                    "timestamp": event["timestamp"],
                }
            )
        elif t == "target_hypothesis_created":
            self.hypotheses[p["hypothesis_id"]] = {**p, "event_id": event["event_id"]}
        elif t == "data_statement_registered":
            self.statements[p["statement_id"]] = {
                **p,
                "event_id": event["event_id"],
            }
        elif t == "license_terms_granted":
            key = (p["statement_id"], p["grantee_party_id"])
            self.licenses.setdefault(key, []).append(
                {
                    "version": p["version"],
                    "purposes": list(p["purposes"]),
                    "valid_from": p["valid_from"],
                    "valid_until": p.get("valid_until"),
                    "confidential_until": p.get("confidential_until"),
                    "event_id": event["event_id"],
                    "recorded_at": event["timestamp"],
                }
            )
        elif t == "model_registered":
            self.models[p["model_id"]] = {**p, "event_id": event["event_id"]}
        elif t == "model_run_recorded":
            self.runs[p["run_id"]] = {**p, "event_id": event["event_id"]}
        elif t == "candidate_registered":
            self.candidates[p["candidate_id"]] = {
                **p,
                "event_id": event["event_id"],
            }
        elif t == "candidate_design_attributed":
            self.design_attributions.append({**p, "event_id": event["event_id"]})
            cand = self.candidates[p["candidate_id"]]
            cand.setdefault("origin", {})["model_run_id"] = p["model_run_id"]
        elif t == "synthesis_batch_recorded":
            self.batches[p["batch_id"]] = {**p, "event_id": event["event_id"]}
        elif t == "measurement_recorded":
            self.measurements[p["measurement_id"]] = {
                **p,
                "event_id": event["event_id"],
                "recorded_at": event["timestamp"],
            }
        elif t == "measurement_corrected":
            self.corrections.append({**p, "event_id": event["event_id"],
                                     "timestamp": event["timestamp"]})
        elif t == "judgment_recorded":
            self.judgments.append({**p, "event_id": event["event_id"],
                                   "timestamp": event["timestamp"]})
        elif t == "stage_gate_frozen":
            self.gates[p["gate_id"]] = {**p, "event_id": event["event_id"],
                                        "frozen_at": event["timestamp"]}

    def _append(self, event_type: str, payload: dict, actor: Optional[dict],
                idempotency_key: Optional[str] = None) -> dict:
        digest = None
        if idempotency_key:
            pending = self._idem_digests[-1] if self._idem_digests else None
            # 摘要必须基于客户端原始请求体（由装饰器压栈），而非规范化后的载荷。
            digest = pending[1] if pending and pending[0] == idempotency_key \
                else self.store.request_digest(payload, actor or {})
            payload = {**payload, "idempotency_key": idempotency_key,
                       "idempotency_digest": digest}
        event, _reused = self.store.append(
            event_type, payload, actor or {},
            idempotency_key=idempotency_key, idempotency_digest=digest,
            on_appended=self._apply,
        )
        return event

    # ================================================================ #
    # 校验辅助
    # ================================================================ #

    @staticmethod
    def _require(payload: dict, fields: list[str]) -> None:
        missing = [f for f in fields if payload.get(f) in (None, "")]
        if missing:
            raise DomainError("missing_fields", f"缺少必填字段：{', '.join(missing)}")

    def _authenticate(self, actor: Optional[dict]) -> dict:
        if not actor or not actor.get("person_id") or not actor.get("party_id"):
            raise DomainError("unauthenticated", "缺少 X-User-Id / X-Party-Id 身份头",
                              status=401)
        person = self.persons.get(actor["person_id"])
        if person is None:
            raise DomainError("unknown_person", "人员未登记", status=401)
        if person["party_id"] != actor["party_id"]:
            raise DomainError("party_mismatch",
                              "人员不属于该合作机构，禁止以该机构身份操作", status=403)
        if actor["party_id"] not in self.parties:
            raise DomainError("unknown_party", "合作机构未登记", status=401)
        return actor

    def _require_event(self, event_id: str, label: str = "证据") -> dict:
        event = self.events_by_id.get(event_id)
        if event is None:
            raise DomainError("unknown_reference", f"{label}不存在：{event_id}",
                              status=404)
        return event

    @staticmethod
    def _unique(entity_id: str, collection: dict, label: str) -> None:
        if entity_id in collection:
            raise DomainError("already_exists", f"{label}已存在：{entity_id}",
                              status=409)

    # ================================================================ #
    # 机构与人员
    # ================================================================ #

    @command
    def register_party(self, payload: dict, actor=None, idem_key=None) -> dict:
        self._require(payload, ["party_id", "name"])
        self._unique(payload["party_id"], self.parties, "合作机构")
        return self._append("party_registered",
                            {"party_id": payload["party_id"], "name": payload["name"]},
                            None, idem_key)

    @command
    def register_person(self, payload: dict, actor=None, idem_key=None) -> dict:
        self._require(payload, ["person_id", "name", "party_id"])
        self._unique(payload["person_id"], self.persons, "人员")
        if payload["party_id"] not in self.parties:
            raise DomainError("unknown_party", "合作机构未登记", status=404)
        return self._append(
            "person_registered",
            {"person_id": payload["person_id"], "name": payload["name"],
             "party_id": payload["party_id"]},
            None, idem_key,
        )

    # ================================================================ #
    # 探索线：分叉与有证据合并
    # ================================================================ #

    @command
    def create_line(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["line_id", "name", "kind"])
        if payload["kind"] not in LINE_KINDS:
            raise DomainError("invalid_kind",
                              f"探索线类型须为 {sorted(LINE_KINDS)}")
        self._unique(payload["line_id"], self.lines, "探索线")
        parent = payload.get("parent_line_id")
        if parent is not None and parent not in self.lines:
            raise DomainError("unknown_line", f"父探索线不存在：{parent}", status=404)
        return self._append(
            "exploration_line_created",
            {"line_id": payload["line_id"], "name": payload["name"],
             "kind": payload["kind"], "parent_line_id": parent,
             "reason": payload.get("reason", "")},
            actor, idem_key,
        )

    @command
    def merge_lines(self, payload: dict, actor: dict, idem_key=None) -> dict:
        """两条探索线合并，必须引用至少一条真实的证据事件。"""
        actor = self._authenticate(actor)
        self._require(payload, ["line_id", "into_line_id",
                                "evidence_event_ids", "justification"])
        source_id, into_id = payload["line_id"], payload["into_line_id"]
        if source_id == into_id:
            raise DomainError("invalid_merge", "不能将探索线合并入自身")
        if source_id not in self.lines or into_id not in self.lines:
            raise DomainError("unknown_line", "待合并的探索线不存在", status=404)
        evidence_ids = payload["evidence_event_ids"]
        if not isinstance(evidence_ids, list) or not evidence_ids:
            raise DomainError("evidence_required", "合并探索线必须提供证据")
        for ref in evidence_ids:
            event = self._require_event(ref, "合并证据")
            if event["type"] not in EVIDENCE_BEARING_TYPES:
                raise DomainError(
                    "evidence_not_supporting",
                    f"事件 {ref}（{event['type']}）不能作为合并依据",
                )
        # 分叉线仍保留：只记录合并关系，不删除源线，竞争方案可继续独立存在。
        return self._append(
            "exploration_lines_merged",
            {"line_id": source_id, "into_line_id": into_id,
             "evidence_event_ids": evidence_ids,
             "justification": payload["justification"]},
            actor, idem_key,
        )

    # ================================================================ #
    # 靶点假设
    # ================================================================ #

    @command
    def create_hypothesis(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["hypothesis_id", "disease", "mechanism", "statement"])
        self._unique(payload["hypothesis_id"], self.hypotheses, "靶点假设")
        line_id = payload.get("line_id")
        if line_id is not None and line_id not in self.lines:
            raise DomainError("unknown_line", f"探索线不存在：{line_id}", status=404)
        refs = payload.get("evidence_refs", [])
        for ref in refs:
            self._require_event(ref, "假设证据")
        return self._append(
            "target_hypothesis_created",
            {"hypothesis_id": payload["hypothesis_id"],
             "disease": payload["disease"], "mechanism": payload["mechanism"],
             "statement": payload["statement"], "line_id": line_id,
             "evidence_refs": refs},
            actor, idem_key,
        )

    # ================================================================ #
    # 数据声明与许可
    # ================================================================ #

    @command
    def register_data_statement(self, payload: dict, actor: dict,
                                idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["statement_id", "name", "dataset_hash"])
        if not is_sha256_hex(payload["dataset_hash"]):
            raise DomainError("invalid_hash", "dataset_hash 须为 64 位十六进制 SHA-256")
        self._unique(payload["statement_id"], self.statements, "训练数据声明")
        return self._append(
            "data_statement_registered",
            {"statement_id": payload["statement_id"], "name": payload["name"],
             "dataset_hash": payload["dataset_hash"],
             "owner_party_id": actor["party_id"],
             "provenance": payload.get("provenance", "")},
            actor, idem_key,
        )

    @command
    def grant_license(self, payload: dict, actor: dict, idem_key=None) -> dict:
        """登记一版许可条款。同一数据/被许可方再次追加即为新版本，

        新版本立即约束之后发生的计算；不追溯、不删除旧版本下已形成的结论。
        """
        actor = self._authenticate(actor)
        self._require(payload, ["statement_id", "grantee_party_id", "version",
                                "purposes", "valid_from"])
        statement = self.statements.get(payload["statement_id"])
        if statement is None:
            raise DomainError("unknown_statement", "训练数据声明不存在", status=404)
        # 只有数据属主可以发放/变更许可。
        if statement["owner_party_id"] != actor["party_id"]:
            raise DomainError("forbidden", "只有数据属主可以变更许可", status=403)
        if payload["grantee_party_id"] not in self.parties:
            raise DomainError("unknown_party", "被许可机构未登记", status=404)
        purposes = payload["purposes"]
        if not isinstance(purposes, list) or not purposes:
            raise DomainError("invalid_license", "purposes 不能为空")
        parse_ts(payload["valid_from"])
        if payload.get("valid_until"):
            parse_ts(payload["valid_until"])
        if payload.get("confidential_until"):
            parse_ts(payload["confidential_until"])
        key = (payload["statement_id"], payload["grantee_party_id"])
        versions = self.licenses.get(key, [])
        if any(v["version"] == payload["version"] for v in versions):
            raise DomainError("version_exists",
                              f"许可版本已存在：{payload['version']}", status=409)
        return self._append(
            "license_terms_granted",
            {"statement_id": payload["statement_id"],
             "grantee_party_id": payload["grantee_party_id"],
             "version": payload["version"], "purposes": purposes,
             "valid_from": payload["valid_from"],
             "valid_until": payload.get("valid_until"),
             "confidential_until": payload.get("confidential_until")},
            actor, idem_key,
        )

    def _current_license(self, statement_id: str, party_id: str) -> Optional[dict]:
        versions = self.licenses.get((statement_id, party_id))
        return versions[-1] if versions else None

    def _assert_license_allows(self, statement_id: str, party_id: str,
                               purpose: str, now: datetime) -> dict:
        license_ = self._current_license(statement_id, party_id)
        if license_ is None:
            raise DomainError(
                "license_missing",
                f"机构 {party_id} 对数据 {statement_id} 无有效许可，禁止该计算",
                status=403,
            )
        if purpose not in license_["purposes"]:
            raise DomainError(
                "license_purpose_denied",
                f"当前许可版本 {license_['version']} 不允许用途 {purpose}，"
                "已阻止本次计算（既有结论保留）",
                status=403,
            )
        if now < parse_ts(license_["valid_from"]):
            raise DomainError("license_not_started", "许可尚未生效", status=403)
        if license_["valid_until"] and now > parse_ts(license_["valid_until"]):
            raise DomainError(
                "license_expired",
                f"许可已于 {license_['valid_until']} 到期，已阻止本次计算"
                "（既有结论保留）",
                status=403,
            )
        return license_

    # ================================================================ #
    # 模型与运行
    # ================================================================ #

    @command
    def register_model(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["model_id", "name", "version", "code_hash",
                                "param_hash", "training_data_hash",
                                "training_statement_ids"])
        self._unique(payload["model_id"], self.models, "模型")
        for field in ("code_hash", "param_hash", "training_data_hash"):
            if not is_sha256_hex(payload[field]):
                raise DomainError("invalid_hash", f"{field} 须为 64 位十六进制 SHA-256")
        statement_ids = payload["training_statement_ids"]
        if not isinstance(statement_ids, list) or not statement_ids:
            raise DomainError("missing_fields", "training_statement_ids 不能为空")
        for sid in statement_ids:
            if sid not in self.statements:
                raise DomainError("unknown_statement",
                                  f"训练数据声明不存在：{sid}", status=404)
        return self._append(
            "model_registered",
            {"model_id": payload["model_id"], "name": payload["name"],
             "version": payload["version"], "code_hash": payload["code_hash"],
             "param_hash": payload["param_hash"],
             "training_data_hash": payload["training_data_hash"],
             "training_statement_ids": statement_ids,
             "hyperparameters": payload.get("hyperparameters", {})},
            actor, idem_key,
        )

    @command
    def record_model_run(self, payload: dict, actor: dict, idem_key=None) -> dict:
        """登记一次模型运行；按当前许可校验，属新计算，受收紧后的条款约束。"""
        actor = self._authenticate(actor)
        self._require(payload, ["run_id", "model_id", "purpose", "ranking"])
        self._unique(payload["run_id"], self.runs, "模型运行")
        model = self.models.get(payload["model_id"])
        if model is None:
            raise DomainError("unknown_model", "模型不存在", status=404)
        line_id = payload.get("line_id")
        if line_id is not None and line_id not in self.lines:
            raise DomainError("unknown_line", f"探索线不存在：{line_id}", status=404)
        ranking = payload["ranking"]
        if not isinstance(ranking, list) or not ranking:
            raise DomainError("missing_fields", "ranking 不能为空")
        # 排名可能先于候选登记产生（算法先出排名，分子后入库）：
        # 这里只校验分值，候选缺席不阻断，登记后由排名自动建立关联。
        for entry in ranking:
            if not isinstance(entry.get("candidate_id"), str) or not entry["candidate_id"]:
                raise DomainError("invalid_ranking", "每个排名项需要 candidate_id")
            if not isinstance(entry.get("score"), (int, float)):
                raise DomainError("invalid_ranking", "每个排名项需要数值型 score")

        now = parse_ts(self.store.now())
        license_snapshot = {}
        for sid in model["training_statement_ids"]:
            statement = self.statements[sid]
            if statement["owner_party_id"] == actor["party_id"]:
                license_snapshot[sid] = {"version": None,
                                         "purposes": ["__owner__"],
                                         "valid_until": None}
                continue
            license_ = self._assert_license_allows(
                sid, actor["party_id"], payload["purpose"], now
            )
            license_snapshot[sid] = {
                "version": license_["version"],
                "purposes": license_["purposes"],
                "valid_until": license_["valid_until"],
            }
        return self._append(
            "model_run_recorded",
            {"run_id": payload["run_id"], "model_id": payload["model_id"],
             "line_id": line_id, "purpose": payload["purpose"],
             "ranking": ranking, "license_snapshot": license_snapshot,
             "ran_by_party_id": actor["party_id"]},
            actor, idem_key,
        )

    # ================================================================ #
    # 候选分子（结构 ACL）
    # ================================================================ #

    @command
    def register_candidate(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["candidate_id", "structure", "line_id"])
        self._unique(payload["candidate_id"], self.candidates, "候选分子")
        if payload["line_id"] not in self.lines:
            raise DomainError("unknown_line", "探索线不存在", status=404)
        visible_to = payload.get("visible_to_party_ids", [])
        if not isinstance(visible_to, list):
            raise DomainError("invalid_acl", "visible_to_party_ids 须为列表")
        for party_id in visible_to:
            if party_id not in self.parties:
                raise DomainError("unknown_party",
                                  f"授权机构未登记：{party_id}", status=404)
        origin = payload.get("origin", {})
        if origin.get("model_run_id") and origin["model_run_id"] not in self.runs:
            raise DomainError("unknown_run", "来源模型运行不存在", status=404)
        if origin.get("hypothesis_id") and origin["hypothesis_id"] not in self.hypotheses:
            raise DomainError("unknown_hypothesis", "来源假设不存在", status=404)
        return self._append(
            "candidate_registered",
            {"candidate_id": payload["candidate_id"],
             "structure": payload["structure"],
             "structure_format": payload.get("structure_format", "smiles"),
             "owner_party_id": actor["party_id"],
             "visible_to_party_ids": visible_to,
             "line_id": payload["line_id"],
             "name": payload.get("name", payload["candidate_id"]),
             "origin": origin},
            actor, idem_key,
        )

    def can_see_structure(self, candidate: dict, viewer_party_id: str) -> bool:
        return (
            viewer_party_id == candidate["owner_party_id"]
            or viewer_party_id in candidate["visible_to_party_ids"]
        )

    def _redact_candidate(self, candidate: dict, viewer_party_id: Optional[str]) -> dict:
        out = {k: v for k, v in candidate.items() if k != "structure"}
        if viewer_party_id and self.can_see_structure(candidate, viewer_party_id):
            out["structure"] = candidate["structure"]
        else:
            out["structure"] = "[REDACTED]"
            out["structure_visible"] = False
        return out

    # ================================================================ #
    # 合成批次、原始测量与追加更正
    # ================================================================ #

    @command
    def attribute_candidate_design(self, payload: dict, actor: dict,
                                   idem_key=None) -> dict:
        """把候选分子的设计归因到某次模型运行（运行常晚于候选登记，故单独追加）。"""
        actor = self._authenticate(actor)
        self._require(payload, ["candidate_id", "model_run_id"])
        candidate = self.candidates.get(payload["candidate_id"])
        if candidate is None:
            raise DomainError("unknown_candidate", "候选分子不存在", status=404)
        run = self.runs.get(payload["model_run_id"])
        if run is None:
            raise DomainError("unknown_run", "模型运行不存在", status=404)
        if not any(e["candidate_id"] == payload["candidate_id"] for e in run["ranking"]):
            raise DomainError("not_in_ranking", "该候选不在此次运行的排名内，不能归因")
        if candidate.get("origin", {}).get("model_run_id") == payload["model_run_id"]:
            raise DomainError("already_exists", "该设计归因已记录", status=409)
        return self._append(
            "candidate_design_attributed",
            {"candidate_id": payload["candidate_id"],
             "model_run_id": payload["model_run_id"]},
            actor, idem_key,
        )

    @command
    def record_batch(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["batch_id", "candidate_id", "protocol_hash"])
        self._unique(payload["batch_id"], self.batches, "合成批次")
        candidate = self.candidates.get(payload["candidate_id"])
        if candidate is None:
            raise DomainError("unknown_candidate", "候选分子不存在", status=404)
        if not is_sha256_hex(payload["protocol_hash"]):
            raise DomainError("invalid_hash", "protocol_hash 须为 64 位十六进制 SHA-256")
        return self._append(
            "synthesis_batch_recorded",
            {"batch_id": payload["batch_id"],
             "candidate_id": payload["candidate_id"],
             "protocol_hash": payload["protocol_hash"],
             "status": payload.get("status", "synthesized"),
             "notes": payload.get("notes", ""),
             "produced_by_party_id": actor["party_id"]},
            actor, idem_key,
        )

    @command
    def record_measurement(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["measurement_id", "batch_id", "assay",
                                "value", "unit"])
        self._unique(payload["measurement_id"], self.measurements, "测量记录")
        batch = self.batches.get(payload["batch_id"])
        if batch is None:
            raise DomainError("unknown_batch", "合成批次不存在", status=404)
        if not isinstance(payload["value"], (int, float)):
            raise DomainError("invalid_measurement", "value 须为数值")
        raw_hash = payload.get("raw_payload_hash")
        if raw_hash is not None and not is_sha256_hex(raw_hash):
            raise DomainError("invalid_hash",
                              "raw_payload_hash 须为 64 位十六进制 SHA-256")
        return self._append(
            "measurement_recorded",
            {"measurement_id": payload["measurement_id"],
             "batch_id": payload["batch_id"],
             "candidate_id": batch["candidate_id"],
             "assay": payload["assay"], "value": payload["value"],
             "unit": payload["unit"],
             "raw_payload_hash": raw_hash,
             "instrument": payload.get("instrument", ""),
             "recorded_by_party_id": actor["party_id"]},
            actor, idem_key,
        )

    @command
    def correct_measurement(self, payload: dict, actor: dict, idem_key=None) -> dict:
        """追加一条更正。原值与原事件保留，当前读数取更正链末端。"""
        actor = self._authenticate(actor)
        self._require(payload, ["measurement_id", "correction_id",
                                "new_value", "reason"])
        measurement = self.measurements.get(payload["measurement_id"])
        if measurement is None:
            raise DomainError("unknown_measurement", "测量记录不存在", status=404)
        if any(c["correction_id"] == payload["correction_id"] for c in self.corrections):
            raise DomainError("already_exists",
                              f"更正已存在：{payload['correction_id']}", status=409)
        if not isinstance(payload["new_value"], (int, float)):
            raise DomainError("invalid_measurement", "new_value 须为数值")
        prior = payload.get("prior_correction_id")
        if prior is not None and not any(
            c["correction_id"] == prior
            and c["measurement_id"] == payload["measurement_id"]
            for c in self.corrections
        ):
            raise DomainError("unknown_correction", "前序更正不存在", status=404)
        refs = payload.get("evidence_refs", [])
        for ref in refs:
            self._require_event(ref, "更正证据")
        return self._append(
            "measurement_corrected",
            {"measurement_id": payload["measurement_id"],
             "correction_id": payload["correction_id"],
             "new_value": payload["new_value"],
             "unit": payload.get("unit", measurement["unit"]),
             "reason": payload["reason"],
             "prior_correction_id": prior, "evidence_refs": refs,
             "corrected_by_party_id": actor["party_id"]},
            actor, idem_key,
        )

    # ================================================================ #
    # 人员判断（继续 / 放弃的理由与反证）
    # ================================================================ #

    @command
    def record_judgment(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["judgment_id", "subject_type", "subject_id",
                                "decision", "rationale"])
        if payload["decision"] not in DECISIONS:
            raise DomainError("invalid_decision",
                              f"decision 须为 {sorted(DECISIONS)}")
        stype, sid = payload["subject_type"], payload["subject_id"]
        bucket = {
            "candidate": self.candidates,
            "hypothesis": self.hypotheses,
            "line": self.lines,
        }.get(stype)
        if bucket is None:
            raise DomainError("invalid_subject",
                              "subject_type 须为 candidate/hypothesis/line")
        if sid not in bucket:
            raise DomainError("unknown_subject", f"{stype} 不存在：{sid}", status=404)
        refs = payload.get("evidence_refs", [])
        counter = payload.get("counter_evidence_refs", [])
        if not isinstance(refs, list) or not refs:
            raise DomainError("evidence_required", "判断必须引用支持证据")
        for ref in refs + counter:
            self._require_event(ref, "判断证据")
        return self._append(
            "judgment_recorded",
            {"judgment_id": payload["judgment_id"], "subject_type": stype,
             "subject_id": sid, "decision": payload["decision"],
             "rationale": payload["rationale"], "evidence_refs": refs,
             "counter_evidence_refs": counter,
             "confidence": payload.get("confidence"),
             "person_id": actor["person_id"],
             "party_id": actor["party_id"]},
            actor, idem_key,
        )

    # ================================================================ #
    # 阶段门：冻结材料 / 利益冲突 / 批准意见
    # ================================================================ #

    def _line_closure(self, line_id: str) -> list[str]:
        """探索线及其分叉祖先、合并来源线（递归）。"""
        out: list[str] = []
        stack = [line_id]
        seen = set()
        while stack:
            current = stack.pop()
            if current in seen or current not in self.lines:
                continue
            seen.add(current)
            out.append(current)
            line = self.lines[current]
            if line.get("parent_line_id"):
                stack.append(line["parent_line_id"])
            for merge in line["merged_from"]:
                stack.append(merge["line_id"])
        return out

    def _line_material_event_ids(self, line_id: str) -> list[str]:
        line_ids = set(self._line_closure(line_id))
        ids: list[str] = []
        for hyp in self.hypotheses.values():
            if hyp.get("line_id") in line_ids:
                ids.append(hyp["event_id"])
        for run in self.runs.values():
            if run.get("line_id") in line_ids:
                ids.append(run["event_id"])
        candidate_ids = {
            c["candidate_id"] for c in self.candidates.values()
            if c["line_id"] in line_ids
        }
        for cand in self.candidates.values():
            if cand["line_id"] in line_ids:
                ids.append(cand["event_id"])
        for batch in self.batches.values():
            if batch["candidate_id"] in candidate_ids:
                ids.append(batch["event_id"])
        for meas in self.measurements.values():
            if meas["candidate_id"] in candidate_ids:
                ids.append(meas["event_id"])
        for corr in self.corrections:
            if corr["measurement_id"] in self.measurements and \
                    self.measurements[corr["measurement_id"]]["candidate_id"] \
                    in candidate_ids:
                ids.append(corr["event_id"])
        for judge in self.judgments:
            if (judge["subject_type"] == "line" and judge["subject_id"] in line_ids) or \
               (judge["subject_type"] == "candidate" and judge["subject_id"] in candidate_ids) or \
               (judge["subject_type"] == "hypothesis"
                    and self.hypotheses.get(judge["subject_id"], {}).get("line_id") in line_ids):
                ids.append(judge["event_id"])
        for merge in self.lines[line_id]["merged_from"]:
            ids.append(merge["event_id"])
        return ids

    def _runs_for_candidate(self, candidate_id: str) -> list[dict]:
        """与候选相关的模型运行：显式归因 + 排名中包含它的全部运行。"""
        run_ids = {
            run["run_id"] for run in self.runs.values()
            if any(e.get("candidate_id") == candidate_id for e in run["ranking"])
        }
        run_id = self.candidates[candidate_id].get("origin", {}).get("model_run_id")
        if run_id:
            run_ids.add(run_id)
        return [self.runs[r] for r in run_ids if r in self.runs]

    def _candidate_material_event_ids(self, candidate_id: str) -> list[str]:
        ids: list[str] = []
        cand = self.candidates[candidate_id]
        ids.append(cand["event_id"])
        for attr in self.design_attributions:
            if attr["candidate_id"] == candidate_id:
                ids.append(attr["event_id"])
        origin = cand.get("origin", {})
        for run in self._runs_for_candidate(candidate_id):
            ids.append(run["event_id"])
            model = self.models[run["model_id"]]
            ids.append(model["event_id"])
            for sid in model["training_statement_ids"]:
                ids.append(self.statements[sid]["event_id"])
                for lic in self.licenses.get((sid, run["ran_by_party_id"]), []):
                    ids.append(lic["event_id"])
            for entry in run["ranking"]:
                if entry["candidate_id"] != candidate_id:
                    other = self.candidates.get(entry["candidate_id"])
                    if other:
                        ids.append(other["event_id"])
        if origin.get("hypothesis_id"):
            ids.append(self.hypotheses[origin["hypothesis_id"]]["event_id"])
        for batch in self.batches.values():
            if batch["candidate_id"] == candidate_id:
                ids.append(batch["event_id"])
        for meas in self.measurements.values():
            if meas["candidate_id"] == candidate_id:
                ids.append(meas["event_id"])
        for corr in self.corrections:
            if self.measurements[corr["measurement_id"]]["candidate_id"] == candidate_id:
                ids.append(corr["event_id"])
        for judge in self.judgments:
            if judge["subject_type"] == "candidate" and judge["subject_id"] == candidate_id:
                ids.append(judge["event_id"])
        return ids

    @command
    def freeze_stage_gate(self, payload: dict, actor: dict, idem_key=None) -> dict:
        actor = self._authenticate(actor)
        self._require(payload, ["gate_id", "stage", "decision", "approvals"])
        self._unique(payload["gate_id"], self.gates, "阶段门")
        if payload["decision"] not in GATE_DECISIONS:
            raise DomainError("invalid_decision",
                              f"门决议须为 {sorted(GATE_DECISIONS)}")
        line_id = payload.get("line_id")
        candidate_ids = payload.get("candidate_ids", [])
        if not line_id and not candidate_ids:
            raise DomainError("missing_scope", "阶段门必须指定 line_id 或 candidate_ids")
        if line_id and line_id not in self.lines:
            raise DomainError("unknown_line", "探索线不存在", status=404)
        for cid in candidate_ids:
            if cid not in self.candidates:
                raise DomainError("unknown_candidate",
                                  f"候选分子不存在：{cid}", status=404)

        approvals = payload["approvals"]
        if not isinstance(approvals, list) or not approvals:
            raise DomainError("approvals_required", "阶段门必须记录批准意见")
        for approval in approvals:
            person = self.persons.get(approval.get("person_id"))
            if person is None:
                raise DomainError("unknown_person",
                                  f"批准人未登记：{approval.get('person_id')}",
                                  status=404)
            if approval.get("vote") not in ("approve", "reject", "abstain"):
                raise DomainError("invalid_vote",
                                  "vote 须为 approve/reject/abstain")
        cois = payload.get("conflicts_of_interest", [])
        for coi in cois:
            if self.persons.get(coi.get("person_id")) is None:
                raise DomainError("unknown_person",
                                  f"利益冲突声明人未登记：{coi.get('person_id')}",
                                  status=404)
            if not coi.get("declaration"):
                raise DomainError("coi_declaration_required",
                                  "利益冲突必须包含 declaration 文本")

        # 汇总冻结材料：范围自动展开 + 调用方显式补充，去重后逐件绑定哈希。
        # 整段在追加锁内完成，保证冻结头就是冻结事件自身的 prev_hash。
        with self.store.write_lock:
            material_ids: list[str] = []
            if line_id:
                material_ids.extend(self._line_material_event_ids(line_id))
            for cid in candidate_ids:
                material_ids.extend(self._candidate_material_event_ids(cid))
            for ref in payload.get("extra_material_ids", []):
                material_ids.append(self._require_event(ref, "冻结材料")["event_id"])
            material_ids = list(dict.fromkeys(material_ids))
            materials = [
                {"event_id": eid, "type": self.events_by_id[eid]["type"],
                 "hash": self.events_by_id[eid]["hash"]}
                for eid in material_ids
            ]
            materials_head = self.store.current_head()
            return self._append(
                "stage_gate_frozen",
                {"gate_id": payload["gate_id"], "stage": payload["stage"],
                 "line_id": line_id, "candidate_ids": candidate_ids,
                 "decision": payload["decision"],
                 "materials": materials, "materials_head": materials_head,
                 "conflicts_of_interest": cois, "approvals": approvals,
                 "summary": payload.get("summary", "")},
                actor, idem_key,
            )

    def verify_gate(self, gate_id: str) -> dict:
        gate = self.gates.get(gate_id)
        if gate is None:
            raise DomainError("unknown_gate", "阶段门不存在", status=404)
        problems: list[str] = []
        for item in gate["materials"]:
            event = self.events_by_id.get(item["event_id"])
            if event is None:
                problems.append(f"材料事件缺失：{item['event_id']}")
            elif event["hash"] != item["hash"]:
                problems.append(f"材料哈希已变化：{item['event_id']}")
        # 冻结头必须是当前链上曾经出现过的前缀头。
        heads: set[str] = set()
        prev = "0" * 64
        for event in self.store.events():
            prev = event["hash"]
            heads.add(prev)
        if gate["materials_head"] not in heads:
            problems.append("冻结时链头不在当前哈希链上（链历史可能被改写）")
        frozen_event = self.events_by_id[gate["event_id"]]
        if frozen_event["prev_hash"] != gate["materials_head"]:
            problems.append("冻结事件未紧随冻结头（冻结后材料可能被增删）")
        return {
            "gate_id": gate_id,
            "ok": not problems,
            "problems": problems,
            "frozen_at": gate["frozen_at"],
            "material_count": len(gate["materials"]),
        }

    # ================================================================ #
    # 反查与复现
    # ================================================================ #

    def trace_candidate(self, candidate_id: str,
                        viewer_party_id: Optional[str]) -> dict:
        cand = self.candidates.get(candidate_id)
        if cand is None:
            raise DomainError("unknown_candidate", "候选分子不存在", status=404)

        # 测量 + 更正链（原值永远在，更正追加在后）
        measurements = []
        for meas in self.measurements.values():
            if meas["candidate_id"] != candidate_id:
                continue
            chain = [c for c in self.corrections
                     if c["measurement_id"] == meas["measurement_id"]]
            chain.sort(key=lambda c: c["timestamp"])
            current = chain[-1] if chain else None
            measurements.append({
                "measurement_id": meas["measurement_id"],
                "assay": meas["assay"],
                "original": {"value": meas["value"], "unit": meas["unit"],
                             "event_id": meas["event_id"],
                             "recorded_at": meas["recorded_at"],
                             "raw_payload_hash": meas.get("raw_payload_hash")},
                "corrections": [
                    {"correction_id": c["correction_id"], "value": c["new_value"],
                     "unit": c["unit"], "reason": c["reason"],
                     "evidence_refs": c["evidence_refs"],
                     "event_id": c["event_id"], "timestamp": c["timestamp"]}
                    for c in chain
                ],
                "current_value": (
                    {"value": current["new_value"], "unit": current["unit"],
                     "via_correction_id": current["correction_id"]}
                    if current else
                    {"value": meas["value"], "unit": meas["unit"],
                     "via_correction_id": None}
                ),
            })

        judgments = [
            {"judgment_id": j["judgment_id"], "decision": j["decision"],
             "rationale": j["rationale"], "evidence_refs": j["evidence_refs"],
             "counter_evidence_refs": j["counter_evidence_refs"],
             "confidence": j["confidence"], "person_id": j["person_id"],
             "party_id": j["party_id"], "timestamp": j["timestamp"],
             "event_id": j["event_id"]}
            for j in self.judgments
            if j["subject_type"] == "candidate" and j["subject_id"] == candidate_id
        ]

        lineage_origin = {}
        origin = cand.get("origin", {})

        def describe_run(run: dict) -> dict:
            model = self.models[run["model_id"]]
            rank_entry = next(
                (e for e in run["ranking"] if e["candidate_id"] == candidate_id), {}
            )
            return {
                "run_id": run["run_id"], "purpose": run["purpose"],
                "score": rank_entry.get("score"), "run_event_id": run["event_id"],
                "ran_by_party_id": run["ran_by_party_id"],
                "is_attributed_origin":
                    origin.get("model_run_id") == run["run_id"],
                "license_snapshot_at_run": run["license_snapshot"],
                "model": {
                    "model_id": model["model_id"], "name": model["name"],
                    "version": model["version"], "code_hash": model["code_hash"],
                    "param_hash": model["param_hash"],
                    "training_data_hash": model["training_data_hash"],
                    "hyperparameters": model["hyperparameters"],
                    "training_statements": [
                        {
                            "statement_id": sid,
                            "dataset_hash": self.statements[sid]["dataset_hash"],
                            "license_version_at_run":
                                run["license_snapshot"].get(sid, {}).get("version"),
                            "current_license_version":
                                (self._current_license(sid, run["ran_by_party_id"])
                                 or {}).get("version"),
                        }
                        for sid in model["training_statement_ids"]
                    ],
                },
            }

        related_runs = self._runs_for_candidate(candidate_id)
        run_infos = [describe_run(r) for r in related_runs]
        # 主设计运行：显式归因优先，否则取排名中最早的一次。
        run_info = next(
            (r for r in run_infos if r["is_attributed_origin"]),
            run_infos[0] if run_infos else None,
        )
        if origin.get("hypothesis_id"):
            hyp = self.hypotheses[origin["hypothesis_id"]]
            lineage_origin["hypothesis"] = {
                "hypothesis_id": hyp["hypothesis_id"], "disease": hyp["disease"],
                "mechanism": hyp["mechanism"], "statement": hyp["statement"],
                "evidence_refs": hyp["evidence_refs"],
                "event_id": hyp["event_id"],
            }

        gates = []
        for gate in self.gates.values():
            included = candidate_id in gate.get("candidate_ids", []) or (
                gate.get("line_id")
                and cand["line_id"] in set(self._line_closure(gate["line_id"]))
            )
            if included:
                gates.append({
                    "gate_id": gate["gate_id"], "stage": gate["stage"],
                    "decision": gate["decision"], "frozen_at": gate["frozen_at"],
                    "materials_head": gate["materials_head"],
                    "approvals": gate["approvals"],
                    "conflicts_of_interest": gate["conflicts_of_interest"],
                    "verify": self.verify_gate(gate["gate_id"]),
                })

        preclinical = next(
            (g for g in gates if g["stage"] == "preclinical" and g["decision"] == "go"),
            None,
        )

        return {
            "candidate": self._redact_candidate(cand, viewer_party_id),
            "exploration_line": self._line_summary(cand["line_id"]),
            "origin": lineage_origin,
            "model_evidence": run_info,
            "all_model_runs": run_infos,
            "synthesis_batches": [
                {"batch_id": b["batch_id"], "status": b["status"],
                 "protocol_hash": b["protocol_hash"], "event_id": b["event_id"]}
                for b in self.batches.values() if b["candidate_id"] == candidate_id
            ],
            "measurements": measurements,
            "judgments": judgments,
            "stage_gates": gates,
            "entered_preclinical": preclinical is not None,
            "preclinical_gate_id": preclinical["gate_id"] if preclinical else None,
            "reproduction": {
                "chain_head": self.store.current_head(),
                "tip": "重新回放 events.jsonl 并按各 event_id 取 payload，"
                       "即可逐字节重现以上全部结论；模型用 code/param/training_data "
                       "三个哈希唯一定位。",
            },
        }

    def _line_summary(self, line_id: str) -> dict:
        line = self.lines[line_id]
        return {
            "line_id": line_id, "name": line["name"], "kind": line["kind"],
            "parent_line_id": line.get("parent_line_id"),
            "merged_from": line["merged_from"],
        }

    def gate_package(self, gate_id: str, viewer_party_id: Optional[str]) -> dict:
        gate = self.gates.get(gate_id)
        if gate is None:
            raise DomainError("unknown_gate", "阶段门不存在", status=404)
        records = []
        for item in gate["materials"]:
            event = self.events_by_id[item["event_id"]]
            payload = dict(event["payload"])
            # 冻结包对外导出同样执行结构 ACL。
            if event["type"] == "candidate_registered":
                cid = payload["candidate_id"]
                if not (viewer_party_id
                        and self.can_see_structure(self.candidates[cid],
                                                   viewer_party_id)):
                    payload["structure"] = "[REDACTED]"
            records.append({
                "event_id": event["event_id"], "type": event["type"],
                "timestamp": event["timestamp"], "hash": event["hash"],
                "payload": payload,
            })
        return {
            "gate_id": gate_id, "stage": gate["stage"],
            "decision": gate["decision"], "frozen_at": gate["frozen_at"],
            "line_id": gate.get("line_id"),
            "candidate_ids": gate.get("candidate_ids", []),
            "materials_head": gate["materials_head"],
            "conflicts_of_interest": gate["conflicts_of_interest"],
            "approvals": gate["approvals"],
            "summary": gate.get("summary", ""),
            "materials": records,
            "verification": self.verify_gate(gate_id),
        }
