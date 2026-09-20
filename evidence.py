"""AI药研决策证据链的领域核心：不可变证据图谱、许可执行与反查。

设计要点
--------
* 互引图谱：靶点假设、训练数据声明、模型与参数哈希、候选分子、合成批次、
  实验结果与人员判断统一建模为不可变实体，以带类型的引用彼此相连，
  引用目标必须已入账（引用完整性）。
* 追加式更正：任何实体（尤其是原始测量）只能以追加新版本的方式更正，
  历史版本永久保留，覆盖在结构上不可能发生。
* 幂等入账：合作方上传凭幂等键一次入账，重试返回首次结果，不重复计数。
* 许可执行：许可（用途、密级、保密期）本身是带版本的证据实体；计算在
  发生时逐条校验并把法律依据快照写进推导记录。政策收紧只阻断新的不合规
  计算，当时合法形成的结论连同其法律依据一并保留。许可义务沿推导链向
  下游继承（用受约束数据训出的模型，继续受同样的约束）。
* 探索线：疾病机制与分子设计等线索可分叉保留竞争方案，也可凭证据合并；
  分支视图按分叉点与合并点精确计算。
* 阶段门：评审冻结材料清单（逐条内容哈希）、利益冲突声明与批准意见，
  之后任何更正都不改变委员会当时所见的版本。
* 反查重现：从候选物出发沿出边、更正链与解释性入边求闭包，完整重现
  所用数据、模型、反证与人工取舍；按查看方密级对无权可见内容（如他方
  化合物结构）打码。
"""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

# ---------------------------------------------------------------- 基础工具


def _canonical(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _hash(obj) -> str:
    return hashlib.sha256(_canonical(obj)).hexdigest()


def _new_id() -> str:
    return uuid.uuid4().hex


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _parse_time(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------- 词汇表

ENTITY_TYPES = frozenset({
    "target_hypothesis",   # 靶点假设
    "data_declaration",    # 训练数据声明
    "model_version",       # 模型与参数哈希
    "candidate_molecule",  # 候选分子
    "synthesis_batch",     # 合成批次
    "experiment_result",   # 实验结果（原始测量）
    "judgment",            # 人员判断
    "derivation",          # 一次受政策约束的计算
    "license_grant",       # 许可授予（用途、密级、保密期）
    "stage_gate",          # 阶段门评审冻结记录
    "branch_fork",         # 探索线分叉记录
    "merge",               # 探索线合并记录
})

# 反查时沿“入边”纳入上下文的规则：None 表示该类型的任何入边都纳入，
# 集合表示只纳入这些关系。出边（实体指向其依据）总是全量跟随。
_CONTEXT_INCOMING = {
    "derivation": {"produces"},                          # 是谁算出了它
    "judgment": None,                                    # 关于它的人工取舍
    "experiment_result": {"contradicts", "supports", "measured_on"},  # 反证/支持/测量
    "synthesis_batch": {"synthesized"},                  # 它的合成批次
    "stage_gate": {"reviews"},                           # 评审过它的阶段门
    "merge": {"evidence"},                               # 以它为证据的合并
}

INTERNAL = "internal"
SHARED = "shared"


class EvidenceError(Exception):
    """证据服务领域错误基类。"""


class UnknownEntity(EvidenceError):
    """引用了不存在的实体。"""


class UnknownBranch(EvidenceError):
    """引用了不存在的探索线。"""


class PolicyViolation(EvidenceError):
    """许可用途、密级或保密期不允许本次计算。"""


@dataclass(frozen=True)
class Entity:
    """一条不可变证据记录。任何修改都以追加新版本（corrects 链）完成。"""

    id: str
    seq: int                 # 全局单调序号，分叉/合并点以此定位
    type: str
    payload: dict
    refs: tuple              # ((relation, target_id), ...)，出边指向依据
    compartment: str         # 密级隔舱：internal / shared / partner:X ...
    branch: str              # 所属探索线
    actor: str
    created_at: str
    hash: str                # 内容哈希，阶段门清单与法律依据快照以此钉版
    corrects: str | None = None
    note: str = ""
    idempotency_key: str | None = None

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "seq": self.seq,
            "type": self.type,
            "payload": copy.deepcopy(self.payload),
            "refs": [list(r) for r in self.refs],
            "compartment": self.compartment,
            "branch": self.branch,
            "actor": self.actor,
            "created_at": self.created_at,
            "hash": self.hash,
            "corrects": self.corrects,
            "note": self.note,
        }


class _Branch:
    """探索线簿记：父线、分叉点、以及并入本线的来源与合并点。"""

    __slots__ = ("parent", "at_seq", "merges")

    def __init__(self, parent, at_seq):
        self.parent = parent
        self.at_seq = at_seq
        self.merges = []  # [(source_branch, at_seq)]


class EvidenceStore:
    """追加式证据存储：实体只增不改，更正以新版本链接。"""

    def __init__(self, clock=None):
        self._clock = clock or _utcnow
        self._entities: dict[str, Entity] = {}
        self._seq = 0
        self._backrefs = defaultdict(set)         # target_id -> {(source_id, relation)}
        self._corrected_by: dict[str, str] = {}   # 旧版本 id -> 新版本 id
        self._idempotency: dict[tuple[str, str], str] = {}  # (actor, key) -> entity_id
        self._license_index = defaultdict(list)   # subject -> [license_grant id, ...]
        self._branches: dict[str, _Branch] = {"main": _Branch(None, 0)}

    # ------------------------------------------------------------ 入账

    def _append(self, type_, payload, *, refs=(), actor, compartment=INTERNAL,
                branch="main", at=None, corrects=None, note="", idempotency_key=None):
        if type_ not in ENTITY_TYPES:
            raise EvidenceError(f"未知实体类型: {type_}")
        if branch not in self._branches:
            raise UnknownBranch(f"未知探索线: {branch}")
        if idempotency_key is not None:
            seen = self._idempotency.get((actor, idempotency_key))
            if seen is not None:
                return self._entities[seen], False
        refs = tuple(tuple(r) for r in refs)
        for _rel, target in refs:
            if target not in self._entities:
                raise UnknownEntity(f"引用不存在的实体: {target}")
        if corrects is not None and corrects not in self._entities:
            raise UnknownEntity(f"更正目标不存在: {corrects}")
        self._seq += 1
        body = {
            "type": type_, "payload": payload, "refs": refs,
            "compartment": compartment, "branch": branch,
            "corrects": corrects, "note": note,
        }
        entity = Entity(
            id=_new_id(), seq=self._seq, type=type_, payload=payload,
            refs=refs, compartment=compartment, branch=branch, actor=actor,
            created_at=at or self._clock(), hash=_hash(body),
            corrects=corrects, note=note, idempotency_key=idempotency_key,
        )
        self._entities[entity.id] = entity
        for rel, target in refs:
            self._backrefs[target].add((entity.id, rel))
        if corrects is not None:
            self._corrected_by[corrects] = entity.id
            self._backrefs[corrects].add((entity.id, "corrects"))
        if idempotency_key is not None:
            self._idempotency[(actor, idempotency_key)] = entity.id
        if type_ == "license_grant":
            self._license_index[payload["subject"]].append(entity.id)
        return entity, True

    def ingest(self, type_, payload, *, refs=(), actor, compartment=INTERNAL,
               branch="main", idempotency_key=None, at=None, note=""):
        """登记一条证据实体；同一上传方凭同一幂等键重试时返回首次入账结果。"""
        return self._append(type_, payload, refs=refs, actor=actor, compartment=compartment,
                            branch=branch, at=at, note=note, idempotency_key=idempotency_key)

    # ------------------------------------------------------------ 读取

    def get(self, entity_id) -> Entity:
        try:
            return self._entities[entity_id]
        except KeyError:
            raise UnknownEntity(f"实体不存在: {entity_id}") from None

    def latest(self, entity_id) -> Entity:
        """沿更正链取当前版本。"""
        current = self.get(entity_id)
        while current.id in self._corrected_by:
            current = self._entities[self._corrected_by[current.id]]
        return current

    def history(self, entity_id) -> list[Entity]:
        """取完整更正链（旧→新），原始测量永远可取回。"""
        root = self.get(entity_id)
        while root.corrects is not None:
            root = self._entities[root.corrects]
        chain = [root]
        while chain[-1].id in self._corrected_by:
            chain.append(self._entities[self._corrected_by[chain[-1].id]])
        return chain

    def _root_id(self, entity) -> str:
        while entity.corrects is not None:
            entity = self._entities[entity.corrects]
        return entity.id

    # ------------------------------------------------------------ 追加式更正

    def correct(self, entity_id, new_payload, *, actor, reason, at=None):
        """以追加新版本的方式更正实体；类型与密级不可借此改变，原版本永久保留。"""
        old = self.latest(entity_id)
        entity, _ = self._append(old.type, new_payload, refs=old.refs, actor=actor,
                                 compartment=old.compartment, branch=old.branch,
                                 at=at, corrects=old.id, note=reason)
        return entity

    # ------------------------------------------------------------ 许可

    def register_license(self, *, subject, purposes, compartments, not_before=None,
                         not_after=None, actor, at=None, idempotency_key=None):
        """把许可授予登记为证据实体：用途集合、可用密级、生效/保密期截止。"""
        self.get(subject)
        payload = {
            "subject": subject,
            "purposes": sorted(purposes),
            "compartments": sorted(compartments),
            "not_before": not_before,
            "not_after": not_after,
        }
        return self._append("license_grant", payload, refs=(("governs", subject),),
                            actor=actor, compartment=SHARED, at=at,
                            idempotency_key=idempotency_key)

    def amend_license(self, license_id, *, actor, reason, at=None, **changes):
        """修订许可：与一切更正一样追加新版本，旧版本（他人的法律依据）保留。"""
        old = self.latest(license_id)
        if old.type != "license_grant":
            raise EvidenceError("只能修订许可实体")
        payload = dict(old.payload)
        for key in ("purposes", "compartments"):
            if key in changes:
                payload[key] = sorted(changes[key])
        for key in ("not_before", "not_after"):
            if key in changes:
                payload[key] = changes[key]
        return self.correct(old.id, payload, actor=actor, reason=reason, at=at)

    def _licenses_for(self, entity_id) -> list[Entity]:
        """实体当前适用的许可（含更正链根与沿推导链继承的许可主体）。"""
        entity = self.get(entity_id)
        subjects = {entity_id, self._root_id(entity),
                    *entity.payload.get("inherited_subjects", ())}
        tips = {}
        for subject in subjects:
            for lic_id in self._license_index.get(subject, ()):
                tip = self.latest(lic_id)
                tips[tip.id] = tip
        return list(tips.values())

    @staticmethod
    def _enforce_license(license_entity, *, purpose, compartment, at):
        p = license_entity.payload
        if purpose not in p["purposes"]:
            raise PolicyViolation(f"许可 {license_entity.id} 不允许用途「{purpose}」")
        if compartment not in p["compartments"]:
            raise PolicyViolation(f"许可 {license_entity.id} 不覆盖密级「{compartment}」")
        if p.get("not_before") and _parse_time(at) < _parse_time(p["not_before"]):
            raise PolicyViolation(f"许可 {license_entity.id} 尚未生效")
        if p.get("not_after") and _parse_time(at) > _parse_time(p["not_after"]):
            raise PolicyViolation(f"许可 {license_entity.id} 已过保密期/有效期")

    # ------------------------------------------------------------ 受政策约束的计算

    def derive(self, *, kind, inputs, purpose, actor, compartment, outputs=(),
               model=None, params_hash=None, extra_refs=(), branch="main",
               idempotency_key=None, at=None):
        """记录一次计算：先校验许可，再原子地登记产出与推导记录。

        校验失败时不落任何记录；成功后推导记录内嵌法律依据快照
        （许可版本 id + 内容哈希），许可义务随 inherited_subjects 沿
        推导链传递给产出实体，使下游计算继续受同样的用途与保密期约束。
        返回 (derivation, output_ids, created)。
        """
        at = at or self._clock()
        if idempotency_key is not None:
            seen = self._idempotency.get((actor, idempotency_key))
            if seen is not None:
                cached = self._entities[seen]
                produced = [t for r, t in cached.refs if r == "produces"]
                return cached, produced, False
        # 先全部校验，保证政策拒绝或参数错误时不落任何记录。
        if branch not in self._branches:
            raise UnknownBranch(f"未知探索线: {branch}")
        if model is not None:
            self.get(model)
        for input_id in inputs:
            self.get(input_id)
        for spec in outputs:
            if spec.get("type") not in ENTITY_TYPES:
                raise EvidenceError(f"未知实体类型: {spec.get('type')}")
            for _rel, target in spec.get("refs", ()):
                self.get(target)
        for _rel, target in extra_refs:
            self.get(target)
        basis, inherited = [], set()
        for input_id in inputs:
            entity = self.get(input_id)
            inherited |= set(entity.payload.get("inherited_subjects", ()))
            for lic in self._licenses_for(input_id):
                self._enforce_license(lic, purpose=purpose, compartment=compartment, at=at)
                basis.append({"license": lic.id, "hash": lic.hash,
                              "subject": lic.payload["subject"]})
        inherited |= {b["subject"] for b in basis}
        output_ids = []
        for spec in outputs:
            out_payload = dict(spec.get("payload", {}))
            if inherited:
                out_payload["inherited_subjects"] = sorted(inherited)
            entity, _ = self._append(
                spec["type"], out_payload, refs=spec.get("refs", ()), actor=actor,
                compartment=spec.get("compartment", compartment), branch=branch, at=at)
            output_ids.append(entity.id)
        refs = ([("consumes", i) for i in inputs]
                + [("produces", o) for o in output_ids]
                + ([("uses_model", model)] if model is not None else [])
                + [tuple(r) for r in extra_refs])
        payload = {
            "kind": kind, "purpose": purpose, "model": model,
            "params_hash": params_hash, "legal_basis": basis,
            "inherited_subjects": sorted(inherited),
        }
        entity, _ = self._append("derivation", payload, refs=refs, actor=actor,
                                 compartment=compartment, branch=branch, at=at,
                                 idempotency_key=idempotency_key)
        return entity, output_ids, True

    # ------------------------------------------------------------ 探索线

    def fork(self, new_branch, *, from_branch, rationale, actor, at=None):
        """分叉一条探索线：保留竞争方案，各自继续积累证据。"""
        if new_branch in self._branches:
            raise EvidenceError(f"探索线已存在: {new_branch}")
        if from_branch not in self._branches:
            raise UnknownBranch(f"未知探索线: {from_branch}")
        entity, _ = self._append(
            "branch_fork",
            {"new_branch": new_branch, "from_branch": from_branch, "rationale": rationale},
            actor=actor, branch=from_branch, at=at)
        self._branches[new_branch] = _Branch(from_branch, self._seq)
        return entity

    def merge(self, target, *, source, evidence, rationale, actor, at=None):
        """把来源线并入目标线：合并记录引用支撑证据，来源线本身保留可查。"""
        for name in (target, source):
            if name not in self._branches:
                raise UnknownBranch(f"未知探索线: {name}")
        refs = tuple(("evidence", e) for e in evidence)
        entity, _ = self._append(
            "merge", {"source": source, "target": target, "rationale": rationale},
            refs=refs, actor=actor, branch=target, at=at)
        self._branches[target].merges.append((source, self._seq))
        return entity

    def branch_view(self, branch, at_seq=None):
        """探索线在某时刻可见的实体集合（含继承自分叉点与合并点的部分）。"""
        if branch not in self._branches:
            raise UnknownBranch(f"未知探索线: {branch}")
        return self._view_at(branch, at_seq if at_seq is not None else self._seq, set())

    def _view_at(self, branch, seq, seen):
        key = (branch, seq)
        if key in seen:
            return set()
        seen.add(key)
        info = self._branches[branch]
        ids = {e.id for e in self._entities.values()
               if e.branch == branch and e.seq <= seq}
        if info.parent is not None and info.at_seq <= seq:
            ids |= self._view_at(info.parent, min(info.at_seq, seq), seen)
        for source, merge_seq in info.merges:
            if merge_seq <= seq:
                ids |= self._view_at(source, min(merge_seq, seq), seen)
        return ids

    # ------------------------------------------------------------ 阶段门

    def freeze_stage_gate(self, *, name, branch, packet, coi=(), approvals=(), actor, at=None):
        """冻结一次阶段门评审：材料清单逐条钉在内容哈希上，附利益冲突与批准意见。

        清单钉的是具体版本 id：会后即使材料被追加更正，委员会当时所见
        仍可原样取回。
        """
        manifest, refs = [], []
        for entity_id in packet:
            entity = self.get(entity_id)
            manifest.append({"entity": entity.id, "hash": entity.hash})
            refs.append(("reviews", entity.id))
        payload = {
            "name": name, "branch": branch, "manifest": manifest,
            "coi": [dict(c) for c in coi],
            "approvals": [dict(a) for a in approvals],
        }
        entity, _ = self._append("stage_gate", payload, refs=refs, actor=actor,
                                 compartment=INTERNAL, branch=branch, at=at)
        return entity

    def verify_stage_gate(self, gate_id) -> bool:
        """复核冻结清单：每条材料的当前存储内容哈希须与冻结时一致。"""
        gate = self.get(gate_id)
        if gate.type != "stage_gate":
            raise EvidenceError("不是阶段门记录")
        for entry in gate.payload["manifest"]:
            entity = self._entities.get(entry["entity"])
            if entity is None or entity.hash != entry["hash"]:
                return False
        return True

    # ------------------------------------------------------------ 反查与打码

    def traceback(self, entity_id, *, viewer=None):
        """从任一实体反查：重现其数据、模型、反证与人工取舍的完整子图。

        闭包沿三个方向扩展：出边（它依据谁）、更正链（两个方向）、
        解释性入边（谁算出了它、谁判断了它、什么实验支持或反驳它、
        哪个阶段门评审过它）。viewer 为可见密级集合，None 表示内部
        全量视角；无权可见的实体以打码占位符返回，载荷与引用不外泄。
        """
        root = self.get(entity_id)
        included: dict[str, Entity] = {}
        queue = [root]
        while queue:
            entity = queue.pop()
            if entity.id in included:
                continue
            included[entity.id] = entity
            for _rel, target in entity.refs:
                if target not in included:
                    queue.append(self._entities[target])
            if entity.corrects is not None and entity.corrects not in included:
                queue.append(self._entities[entity.corrects])
            nxt = self._corrected_by.get(entity.id)
            if nxt is not None and nxt not in included:
                queue.append(self._entities[nxt])
            for source_id, rel in self._backrefs.get(entity.id, ()):
                source = self._entities[source_id]
                allowed = _CONTEXT_INCOMING.get(source.type)
                if source_id not in included and (allowed is None and source.type in _CONTEXT_INCOMING
                                                  or allowed is not None and rel in allowed):
                    queue.append(source)
        edges = set()
        for entity in included.values():
            for rel, target in entity.refs:
                if target in included:
                    edges.add((entity.id, rel, target))
            if entity.corrects is not None and entity.corrects in included:
                edges.add((entity.id, "corrects", entity.corrects))
        entities = [self._public(e, viewer)
                    for e in sorted(included.values(), key=lambda e: e.seq)]
        return {"root": root.id, "entities": entities,
                "edges": sorted(list(e) for e in edges)}

    def public(self, entity_id, viewer=None) -> dict:
        """按查看方密级返回实体的公开视图（越权则打码）。"""
        return self._public(self.get(entity_id), viewer)

    def branch_view_public(self, branch, viewer=None) -> list:
        """探索线当前可见实体的公开视图，按入账顺序排列。"""
        ids = self.branch_view(branch)
        entities = sorted((self._entities[i] for i in ids), key=lambda e: e.seq)
        return [self._public(e, viewer) for e in entities]

    @staticmethod
    def _public(entity, viewer) -> dict:
        if viewer is None or entity.compartment in viewer:
            return entity.as_dict()
        return {
            "id": entity.id,
            "seq": entity.seq,
            "type": entity.type,
            "compartment": entity.compartment,
            "created_at": entity.created_at,
            "redacted": True,
        }
