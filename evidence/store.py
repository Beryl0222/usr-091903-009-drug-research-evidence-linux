"""证据存储层：仅追加的 JSONL 哈希链。

每条事件包含指向前一条事件的 prev_hash，任何对历史行的插入、删除或改写
都会在 verify_chain() 中暴露。存储从不更新或删除既有行，领域层面的"更正"
通过追加一条 correction 事件实现。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from typing import Callable, Optional

EVENT_FILE = "events.jsonl"
GENESIS_HASH = "0" * 64


class StoreError(Exception):
    """存储层错误，code 供 HTTP 层映射状态码。"""

    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status


def canonical_hash(
    event_id: str,
    event_type: str,
    timestamp: str,
    actor: dict,
    payload: dict,
    prev_hash: str,
) -> str:
    body = json.dumps(
        {
            "event_id": event_id,
            "type": event_type,
            "timestamp": timestamp,
            "actor": actor,
            "payload": payload,
            "prev_hash": prev_hash,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


class AppendOnlyStore:
    """单进程、线程安全的追加事件存储。"""

    def __init__(self, directory: str, time_func: Callable[[], str] = None):
        self.directory = directory
        self.path = os.path.join(directory, EVENT_FILE)
        self._lock = threading.RLock()
        self._time_func = time_func or (
            lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        )
        os.makedirs(directory, exist_ok=True)
        # 幂等键 -> 已有事件的精简摘要，启动时重放建立。
        self.idempotency: dict[str, dict] = {}
        self._replay_idempotency()

    # ---- 读取 ----------------------------------------------------------

    def replay(self, callback: Callable[[dict], None]) -> int:
        """按存储顺序把每条事件交给回调，返回事件数。"""
        count = 0
        if not os.path.exists(self.path):
            return 0
        with open(self.path, "r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise StoreError(
                        "chain_corrupt",
                        f"第 {line_no} 行不是合法事件 JSON：{exc}",
                        status=500,
                    ) from exc
                callback(event)
                count += 1
        return count

    def events(self) -> list[dict]:
        out: list[dict] = []
        self.replay(out.append)
        return out

    def verify_chain(self) -> dict:
        """重算全部哈希，返回完整性报告。"""
        prev = GENESIS_HASH
        count = 0
        seen_ids: set[str] = set()
        try:
            def check(event: dict) -> None:
                nonlocal prev, count
                for key in ("event_id", "type", "timestamp", "actor", "payload",
                            "prev_hash", "hash"):
                    if key not in event:
                        raise StoreError(
                            "chain_corrupt",
                            f"事件缺少字段 {key}",
                            status=500,
                        )
                if event["event_id"] in seen_ids:
                    raise StoreError(
                        "chain_corrupt",
                        f"事件 ID 重复：{event['event_id']}",
                        status=500,
                    )
                seen_ids.add(event["event_id"])
                if event["prev_hash"] != prev:
                    raise StoreError(
                        "chain_break",
                        f"事件 {event['event_id']} 的 prev_hash 与链上一条不一致"
                        "（历史可能被插入或删除）",
                        status=500,
                    )
                expected = canonical_hash(
                    event["event_id"], event["type"], event["timestamp"],
                    event["actor"], event["payload"], event["prev_hash"],
                )
                if event["hash"] != expected:
                    raise StoreError(
                        "chain_tampered",
                        f"事件 {event['event_id']} 内容哈希不匹配"
                        "（历史可能被改写）",
                        status=500,
                    )
                prev = event["hash"]
                count += 1

            self.replay(check)
        except StoreError:
            return {"ok": False, "events": count, "head": prev}
        return {"ok": True, "events": count, "head": prev}

    # ---- 写入 ----------------------------------------------------------

    def append(
        self,
        event_type: str,
        payload: dict,
        actor: dict,
        idempotency_key: Optional[str] = None,
        idempotency_digest: Optional[str] = None,
        on_appended: Optional[Callable[[dict], None]] = None,
    ) -> tuple[dict, bool]:
        """追加事件，返回 (事件, 是否复用了既有事件)。

        同一 idempotency_key 的重放：请求摘要一致则返回原事件（一次入账），
        摘要不同则拒绝（幂等键冲突）。新事件在锁内落盘并触发 on_appended，
        保证投影更新顺序与链顺序一致。
        """
        with self._lock:
            if idempotency_key is not None:
                existing = self.idempotency.get(idempotency_key)
                if existing is not None:
                    if existing["digest"] == idempotency_digest:
                        return existing["event"], True
                    raise StoreError(
                        "idempotency_conflict",
                        "幂等键已用于另一请求，不得重复入账",
                        status=409,
                    )

            head = self.current_head()
            event_id = uuid.uuid4().hex
            timestamp = self._time_func()
            event_hash = canonical_hash(
                event_id, event_type, timestamp, actor, payload, head
            )
            event = {
                "event_id": event_id,
                "type": event_type,
                "timestamp": timestamp,
                "actor": actor,
                "payload": payload,
                "prev_hash": head,
                "hash": event_hash,
            }
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            if idempotency_key is not None:
                self.idempotency[idempotency_key] = {
                    "digest": idempotency_digest,
                    "event": event,
                }
            if on_appended is not None:
                on_appended(event)
            return event, False

    # ---- 内部 ----------------------------------------------------------

    @property
    def write_lock(self):
        """暴露追加锁，供"先读链头再追加"的复合操作原子化。"""
        return self._lock

    def now(self) -> str:
        """当前时间（与事件时间戳同一时钟，可在测试中注入）。"""
        return self._time_func()

    def current_head(self) -> str:
        prev = GENESIS_HASH
        if not os.path.exists(self.path):
            return prev
        with open(self.path, "rb") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    prev = json.loads(line)["hash"]
        return prev

    def peek_idempotent(self, key: str) -> Optional[tuple[dict, str]]:
        hit = self.idempotency.get(key)
        if hit is None:
            return None
        return hit["event"], hit["digest"]

    @staticmethod
    def request_digest(payload: dict, actor: dict) -> str:
        return AppendOnlyStore._request_digest(payload, actor)

    def _replay_idempotency(self) -> None:
        def collect(event: dict) -> None:
            key = event.get("payload", {}).get("idempotency_key")
            if key:
                self.idempotency[key] = {
                    "digest": event["payload"].get(
                        "idempotency_digest"
                    ) or self._request_digest(event["payload"], event["actor"]),
                    "event": event,
                }
        self.replay(collect)

    @staticmethod
    def _request_digest(payload: dict, actor: dict) -> str:
        body = json.dumps(
            {"payload": payload, "actor": actor},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(body).hexdigest()
