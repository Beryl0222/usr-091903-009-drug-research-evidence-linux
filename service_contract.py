"""验证基础服务在领域功能开发前保持可运行，并抽查关键 API 契约。"""

import json
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import service
from service import Handler, SERVICE_ID, SERVICE_NAME, health_payload


class ServiceContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from http.server import ThreadingHTTPServer
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _post(self, path, body):
        request = Request(
            f"{self.base_url}{path}", method="POST",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            payload = error.read()
            code = error.code
            error.close()
            return code, json.loads(payload or b"{}")

    def _get(self, path):
        try:
            with urlopen(f"{self.base_url}{path}", timeout=2) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            code = error.code
            error.close()
            return code, {}

    def test_health_payload_has_stable_identity(self):
        self.assertEqual(
            health_payload(),
            {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME},
        )

    def test_health_endpoint_returns_json(self):
        with urlopen(f"{self.base_url}/health", timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers.get_content_type(), "application/json")
            self.assertEqual(json.load(response), health_payload())

    def test_unknown_route_is_not_exposed(self):
        with self.assertRaises(HTTPError) as error:
            urlopen(f"{self.base_url}/unknown", timeout=2)
        self.assertEqual(error.exception.code, 404)
        error.exception.close()

    def test_entity_ingest_is_idempotent_over_http(self):
        service.reset_store()
        body = {"type": "data_declaration", "payload": {"dataset": "D1"},
                "actor": "partner:A", "idempotency_key": "up-1"}
        status1, reply1 = self._post("/entities", body)
        status2, reply2 = self._post("/entities", body)
        self.assertEqual((status1, reply1["created"]), (201, True))
        self.assertEqual((status2, reply2["created"]), (200, False))
        self.assertEqual(reply1["entity"]["id"], reply2["entity"]["id"])

    def test_policy_violation_returns_403(self):
        service.reset_store()
        _, reply = self._post("/entities", {
            "type": "data_declaration", "payload": {"dataset": "D1"}, "actor": "partner:A"})
        decl = reply["entity"]["id"]
        self._post("/licenses", {
            "subject": decl, "purposes": ["training"],
            "compartments": ["internal"], "actor": "counsel"})
        status, reply = self._post("/derivations", {
            "kind": "screen", "inputs": [decl], "purpose": "screening",
            "actor": "algo", "compartment": "internal"})
        self.assertEqual(status, 403)
        self.assertIn("error", reply)

    def test_traceback_hides_restricted_structure_over_http(self):
        service.reset_store()
        _, reply = self._post("/entities", {
            "type": "candidate_molecule",
            "payload": {"name": "MOL-1", "structure": "c1ccccc1"},
            "actor": "partner:A", "compartment": "partner:A"})
        mol = reply["entity"]["id"]
        status, graph = self._get(f"/traceback/{mol}?compartments=shared,partner:B")
        self.assertEqual(status, 200)
        self.assertNotIn("c1ccccc1", json.dumps(graph, ensure_ascii=False))
        self.assertTrue(graph["entities"][0]["redacted"])


if __name__ == "__main__":
    unittest.main()
