"""AI药研决策证据链的服务入口：健康检查与证据 API。

领域规则全部在 evidence.py 中实现并单独测试；本模块只做
JSON 编解码、路由与错误映射，便于替换为其他部署形态。
"""

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from evidence import (
    EvidenceError,
    EvidenceStore,
    PolicyViolation,
    UnknownEntity,
)

SERVICE_ID = "drug-research-evidence"
SERVICE_NAME = "AI药研决策证据链"

STORE = EvidenceStore()


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def reset_store():
    """测试与运维巡检用：重置内存证据存储。"""
    global STORE
    STORE = EvidenceStore()


def _require(body, key):
    if key not in body:
        raise EvidenceError(f"缺少必填字段: {key}")
    return body[key]


def _viewer(query):
    """查看方密级上下文；缺省视为内部全量视角（鉴权接入前的前提）。"""
    raw = query.get("compartments", [None])[0]
    if not raw:
        return None
    return set(raw.split(","))


# ---------------------------------------------------------------- 路由处理

def health(_body, _params, _query):
    return 200, health_payload()


def create_entity(body, _params, _query):
    entity, created = STORE.ingest(
        _require(body, "type"), body.get("payload", {}),
        refs=[tuple(r) for r in body.get("refs", [])],
        actor=body.get("actor", "anonymous"),
        compartment=body.get("compartment", "internal"),
        branch=body.get("branch", "main"),
        idempotency_key=body.get("idempotency_key"),
        note=body.get("note", ""),
    )
    return (201 if created else 200), {"entity": entity.as_dict(), "created": created}


def get_entity(_body, params, query):
    entity = STORE.get(params["eid"])
    return 200, {
        "entity": STORE.public(entity.id, _viewer(query)),
        "latest": STORE.latest(entity.id).id,
        "history": [e.id for e in STORE.history(entity.id)],
    }


def correct_entity(body, params, _query):
    entity = STORE.correct(
        params["eid"], _require(body, "payload"),
        actor=body.get("actor", "anonymous"),
        reason=_require(body, "reason"),
    )
    return 201, {"entity": entity.as_dict()}


def entity_history(_body, params, query):
    viewer = _viewer(query)
    return 200, {"history": [STORE.public(e.id, viewer) for e in STORE.history(params["eid"])]}


def register_license(body, _params, _query):
    entity, created = STORE.register_license(
        subject=_require(body, "subject"),
        purposes=body.get("purposes", []),
        compartments=body.get("compartments", []),
        not_before=body.get("not_before"),
        not_after=body.get("not_after"),
        actor=body.get("actor", "anonymous"),
        idempotency_key=body.get("idempotency_key"),
    )
    return (201 if created else 200), {"license": entity.as_dict(), "created": created}


def amend_license(body, params, _query):
    changes = {k: body[k] for k in ("purposes", "compartments", "not_before", "not_after") if k in body}
    entity = STORE.amend_license(
        params["eid"],
        actor=body.get("actor", "anonymous"),
        reason=_require(body, "reason"),
        **changes,
    )
    return 201, {"license": entity.as_dict()}


def derive(body, _params, _query):
    entity, output_ids, created = STORE.derive(
        kind=body.get("kind", "analysis"),
        inputs=body.get("inputs", []),
        purpose=_require(body, "purpose"),
        actor=body.get("actor", "anonymous"),
        compartment=body.get("compartment", "internal"),
        outputs=body.get("outputs", []),
        model=body.get("model"),
        params_hash=body.get("params_hash"),
        extra_refs=[tuple(r) for r in body.get("extra_refs", [])],
        branch=body.get("branch", "main"),
        idempotency_key=body.get("idempotency_key"),
        at=body.get("at"),
    )
    return (201 if created else 200), {
        "derivation": entity.as_dict(),
        "outputs": output_ids,
        "created": created,
    }


def fork_branch(body, _params, _query):
    entity = STORE.fork(
        _require(body, "new_branch"),
        from_branch=_require(body, "from_branch"),
        rationale=body.get("rationale", ""),
        actor=body.get("actor", "anonymous"),
    )
    return 201, {"fork": entity.as_dict()}


def merge_branch(body, _params, _query):
    entity = STORE.merge(
        _require(body, "target"), source=_require(body, "source"),
        evidence=body.get("evidence", []),
        rationale=body.get("rationale", ""),
        actor=body.get("actor", "anonymous"),
    )
    return 201, {"merge": entity.as_dict()}


def branch_view(_body, params, _query):
    return 200, {"branch": params["name"], "entities": STORE.branch_view_public(params["name"])}


def freeze_gate(body, _params, _query):
    entity = STORE.freeze_stage_gate(
        name=_require(body, "name"),
        branch=body.get("branch", "main"),
        packet=_require(body, "packet"),
        coi=body.get("coi", []),
        approvals=body.get("approvals", []),
        actor=body.get("actor", "anonymous"),
    )
    return 201, {"stage_gate": entity.as_dict()}


def get_gate(_body, params, _query):
    gate = STORE.get(params["eid"])
    return 200, {"stage_gate": gate.as_dict(), "verified": STORE.verify_stage_gate(gate.id)}


def traceback_view(_body, params, query):
    return 200, STORE.traceback(params["eid"], viewer=_viewer(query))


ROUTES = [
    ("GET", re.compile(r"/health"), health),
    ("POST", re.compile(r"/entities"), create_entity),
    ("GET", re.compile(r"/entities/(?P<eid>[0-9a-f]+)"), get_entity),
    ("POST", re.compile(r"/entities/(?P<eid>[0-9a-f]+)/corrections"), correct_entity),
    ("GET", re.compile(r"/entities/(?P<eid>[0-9a-f]+)/history"), entity_history),
    ("POST", re.compile(r"/licenses"), register_license),
    ("POST", re.compile(r"/licenses/(?P<eid>[0-9a-f]+)/amendments"), amend_license),
    ("POST", re.compile(r"/derivations"), derive),
    ("POST", re.compile(r"/branches/fork"), fork_branch),
    ("POST", re.compile(r"/branches/merge"), merge_branch),
    ("GET", re.compile(r"/branches/(?P<name>[\w-]+)/view"), branch_view),
    ("POST", re.compile(r"/stage-gates"), freeze_gate),
    ("GET", re.compile(r"/stage-gates/(?P<eid>[0-9a-f]+)"), get_gate),
    ("GET", re.compile(r"/traceback/(?P<eid>[0-9a-f]+)"), traceback_view),
]


def _match(method, path):
    for route_method, pattern, handler in ROUTES:
        if route_method != method:
            continue
        match = pattern.fullmatch(path)
        if match:
            return handler, match.groupdict()
    return None, None


class Handler(BaseHTTPRequestHandler):
    """提供健康检查，并暴露证据领域接口。"""

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        split = urlsplit(self.path)
        handler, params = _match(method, split.path)
        if handler is None:
            self.send_error(404)
            return
        body = {}
        if method == "POST":
            body = self._read_json()
            if body is None:
                return
        try:
            status, payload = handler(body, params, parse_qs(split.query))
        except UnknownEntity as exc:
            self._respond(404, {"error": str(exc)})
            return
        except PolicyViolation as exc:
            self._respond(403, {"error": str(exc)})
            return
        except EvidenceError as exc:
            self._respond(400, {"error": str(exc)})
            return
        self._respond(status, payload)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            self._respond(400, {"error": "请求体不是合法 JSON"})
            return None

    def _respond(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        print("基础检查通过")
        return
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
