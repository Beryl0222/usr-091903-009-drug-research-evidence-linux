"""AI药研决策证据链的运行入口与 HTTP 接口。

接口约定：
- 除 /health 与机构/人员 bootstrap 外，请求须带 X-User-Id、X-Party-Id 身份头。
- 写请求可带 Idempotency-Key 头：同键同体重放只入账一次，同键不同体返回 409。
- 所有领域状态由仅追加哈希链承载，进程重启后从 events.jsonl 重放恢复。
"""

from __future__ import annotations

import argparse
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from evidence.domain import DomainError, EvidenceService
from evidence.store import AppendOnlyStore, StoreError

SERVICE_ID = "drug-research-evidence"
SERVICE_NAME = "AI药研决策证据链"

DATA_DIR = os.environ.get("EVIDENCE_DATA_DIR", "data")


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


# 路由 -> 领域方法名。None 表示该命令不需要身份头。
COMMAND_ROUTES = {
    "/v1/parties": ("register_party", False),
    "/v1/persons": ("register_person", False),
    "/v1/lines": ("create_line", True),
    "/v1/lines/merge": ("merge_lines", True),
    "/v1/hypotheses": ("create_hypothesis", True),
    "/v1/data-statements": ("register_data_statement", True),
    "/v1/licenses": ("grant_license", True),
    "/v1/models": ("register_model", True),
    "/v1/model-runs": ("record_model_run", True),
    "/v1/candidates": ("register_candidate", True),
    "/v1/candidates/attribute-design": ("attribute_candidate_design", True),
    "/v1/batches": ("record_batch", True),
    "/v1/measurements": ("record_measurement", True),
    "/v1/measurements/correct": ("correct_measurement", True),
    "/v1/judgments": ("record_judgment", True),
    "/v1/stage-gates": ("freeze_stage_gate", True),
}

TRACE_RE = re.compile(r"^/v1/candidates/([^/]+)/trace$")
GATE_RE = re.compile(r"^/v1/stage-gates/([^/]+)$")


class ServiceState:
    """持有存储与领域服务，供所有请求线程共享。"""

    def __init__(self, data_dir: str = DATA_DIR):
        self.store = AppendOnlyStore(data_dir)
        self.service = EvidenceService(self.store)


STATE: ServiceState | None = None


def get_state() -> ServiceState:
    global STATE
    if STATE is None:
        STATE = ServiceState()
    return STATE


def reset_state(data_dir: str = DATA_DIR) -> ServiceState:
    """供测试切换数据目录并重建投影。"""
    global STATE
    STATE = ServiceState(data_dir)
    return STATE


class Handler(BaseHTTPRequestHandler):
    """提供健康检查与领域命令/查询接口。"""

    # ---- GET -------------------------------------------------------- #

    def do_GET(self):
        if self.path == "/health":
            self._write_json(200, health_payload())
            return
        trace_match = TRACE_RE.match(self.path)
        gate_match = GATE_RE.match(self.path)
        if not trace_match and not gate_match and self.path != "/v1/verify-chain":
            self._write_json(404, {"error": "not_found",
                                   "message": f"未知路径：{self.path}"})
            return
        state = get_state()
        service = state.service
        try:
            if trace_match:
                actor = self._optional_actor()
                result = service.trace_candidate(
                    trace_match.group(1), actor.get("party_id") if actor else None
                )
                self._write_json(200, result)
                return
            if gate_match:
                actor = self._optional_actor()
                result = service.gate_package(
                    gate_match.group(1), actor.get("party_id") if actor else None
                )
                self._write_json(200, result)
                return
            self._write_json(200, state.store.verify_chain())
        except (DomainError, StoreError) as exc:
            self._write_json(exc.status, {"error": exc.code, "message": str(exc)})

    # ---- POST ------------------------------------------------------- #

    def do_POST(self):
        route = COMMAND_ROUTES.get(self.path)
        if route is None:
            self._write_json(404, {"error": "not_found",
                                   "message": f"未知路径：{self.path}"})
            return
        method_name, requires_auth = route
        state = get_state()
        try:
            payload = self._read_body()
            actor = self._actor() if requires_auth else None
            event = getattr(state.service, method_name)(
                payload, actor=actor,
                idem_key=self.headers.get("Idempotency-Key"),
            )
            self._write_json(201, {"event_id": event["event_id"],
                                   "hash": event["hash"],
                                   "timestamp": event["timestamp"]})
        except (DomainError, StoreError) as exc:
            self._write_json(exc.status, {"error": exc.code, "message": str(exc)})
        except json.JSONDecodeError as exc:
            self._write_json(400, {"error": "invalid_json", "message": str(exc)})

    # ---- 辅助 ------------------------------------------------------- #

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        payload = json.loads(raw.decode("utf-8") or "{}")
        if not isinstance(payload, dict):
            raise DomainError("invalid_body", "请求体必须是 JSON 对象")
        return payload

    def _actor(self) -> dict:
        return {
            "person_id": self.headers.get("X-User-Id"),
            "party_id": self.headers.get("X-Party-Id"),
        }

    def _optional_actor(self) -> dict | None:
        person = self.headers.get("X-User-Id")
        party = self.headers.get("X-Party-Id")
        if not person or not party:
            return None
        return {"person_id": person, "party_id": party}

    def _write_json(self, status: int, body: dict) -> None:
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir", default=DATA_DIR,
                        help="证据事件文件目录（默认 data/，可用 EVIDENCE_DATA_DIR 覆盖）")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        state = reset_state(args.data_dir)
        report = state.store.verify_chain()
        assert report["ok"], f"哈希链校验失败：{report}"
        print(f"基础检查通过；证据链事件 {report['events']} 条，链头 {report['head'][:12]}")
        return
    reset_state(args.data_dir)
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
