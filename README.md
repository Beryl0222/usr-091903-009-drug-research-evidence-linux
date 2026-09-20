# AI药研决策证据链

药物发现决策证据服务：把靶点假设、训练数据声明、模型与参数哈希、候选分子、合成批次、
原始实验结果、人员判断与阶段门批准彼此引用，回答"这个分子为什么继续、那个为什么放弃"。

## 核心保证

1. **仅追加、哈希链式**：所有写入都是 `data/events.jsonl` 中的一条事件，事件含
   `prev_hash` 与自身内容哈希，任何插入/删除/改写都会被 `GET /v1/verify-chain` 发现。
2. **原始测量不可覆盖**：更正只能追加 `measurement_corrected`，原值永久保留，
   当前读数取更正链末端。
3. **一次入账**：写请求带 `Idempotency-Key` 头时，同键同体重放返回原事件，
   同键不同体返回 409；幂等索引落盘，服务重启后仍生效。
4. **许可按"计算发生时"执行**：每次模型运行按当时最新许可版本校验用途与有效期，
   运行内快照所用许可版本。许可收紧或保密期变化只阻止之后的新计算，
   不追溯、不删除当时合法形成的结论。数据属主对自有数据无需自授许可。
5. **结构保密边界**：化合物结构仅属主与显式授权名单可见；反查与阶段门冻结包对
   无权合作方返回 `[REDACTED]`，但证据关系本身仍可审查。
6. **分叉与有证据合并**：探索线可从既有线分叉保留竞争方案；合并必须引用至少一条
   真实的支持性证据事件（假设/运行/测量/更正/判断），合并不删除源线。
7. **阶段门冻结**：每次门评审自动汇总范围内全部材料并逐件绑定哈希，连同冻结时刻
   链头、利益冲突声明、批准意见一起落为一条不可变事件；事后可校验材料是否被改动。
8. **候选物反查复现**：从进入临床前研究的候选物出发，可完整取出假设、训练数据哈希、
   模型代码/参数哈希、所用许可版本、原始测量与更正、支持/反对证据与人工取舍。

## 运行

```bash
python3 service.py --check                      # 配置与哈希链自检
python3 service.py --port 8000 --data-dir data  # 启动服务（也可用 EVIDENCE_DATA_DIR）
curl http://127.0.0.1:8000/health
```

## 请求约定

- 除 `/health`、`/v1/parties`、`/v1/persons` 外，请求均需身份头：
  `X-User-Id`、`X-Party-Id`（人员必须属于该机构，否则 403）。
- 写接口为 `POST` + JSON 请求体；成功返回 `201` 与 `{event_id, hash, timestamp}`。
- 建议所有上传带 `Idempotency-Key`（如合作方上传重试）。
- 错误体统一为 `{"error": "<code>", "message": "..."}`，
  常见码：400 校验失败、401 未认证、403 许可/归属禁止、404 引用不存在、409 冲突。

## 事件与接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/parties` | 登记合作机构（bootstrap，无需身份头） |
| POST | `/v1/persons` | 登记人员及其所属机构 |
| POST | `/v1/lines` | 创建探索线（`kind`: disease_mechanism/molecule_design，可带 `parent_line_id` 分叉） |
| POST | `/v1/lines/merge` | 合并探索线，必须提供 `evidence_event_ids` 与理由 |
| POST | `/v1/hypotheses` | 靶点假设，可挂探索线、引用证据 |
| POST | `/v1/data-statements` | 训练数据声明（属主为调用方，含 `dataset_hash`） |
| POST | `/v1/licenses` | 发放/收紧许可版本（仅数据属主），含用途、生效/到期/保密期 |
| POST | `/v1/models` | 登记模型（`code_hash`/`param_hash`/`training_data_hash` + 训练声明引用） |
| POST | `/v1/model-runs` | 登记一次运行与排名；按当前许可校验，快照许可版本 |
| POST | `/v1/candidates` | 登记候选分子（结构、属主、`visible_to_party_ids` 授权名单） |
| POST | `/v1/candidates/attribute-design` | （可选）显式指定候选设计来自哪次运行；无此事件时，排名会自动建立关联 |
| POST | `/v1/batches` | 合成批次（候选 + 协议哈希） |
| POST | `/v1/measurements` | 原始测量（原始载荷哈希、仪器） |
| POST | `/v1/measurements/correct` | 追加更正（理由必填，可链接前序更正），不覆盖原值 |
| POST | `/v1/judgments` | 人员判断（continue/abandon/hold/advance，支持证据必填，可附反证） |
| POST | `/v1/stage-gates` | 阶段门冻结（材料自动汇总 + 利益冲突 + 批准意见） |
| GET | `/v1/candidates/{id}/trace` | 候选物全链路反查（按身份脱敏结构） |
| GET | `/v1/stage-gates/{id}` | 导出阶段门冻结包（按身份脱敏） |
| GET | `/v1/verify-chain` | 哈希链完整性校验 |

### 阶段门请求示例

```json
{
  "gate_id": "G1",
  "stage": "preclinical",
  "line_id": "L2",
  "candidate_ids": ["C1"],
  "decision": "go",
  "summary": "同意进入临床前研究",
  "conflicts_of_interest": [
    {"person_id": "u2", "declaration": "在该化合物系列持有专利"}
  ],
  "approvals": [
    {"person_id": "u1", "vote": "approve", "comment": "证据齐备"},
    {"person_id": "u2", "vote": "abstain", "comment": "利益冲突回避"}
  ]
}
```

## 最小流程

```bash
# 1) 机构/人员/探索线
curl -s -X POST localhost:8000/v1/parties -H 'Content-Type: application/json' \
  -d '{"party_id":"p1","name":"本公司"}'
curl -s -X POST localhost:8000/v1/persons -H 'Content-Type: application/json' \
  -d '{"person_id":"u1","name":"甲","party_id":"p1"}'
curl -s -X POST localhost:8000/v1/lines \
  -H 'X-User-Id: u1' -H 'X-Party-Id: p1' -H 'Content-Type: application/json' \
  -d '{"line_id":"L2","name":"分子设计","kind":"molecule_design"}'

# 2) 数据声明 -> 许可 -> 模型 -> 候选 -> 运行（详见 README 接口表）
# 3) 批次/测量；需要时 POST /v1/measurements/correct 追加更正
# 4) 继续/放弃：POST /v1/judgments（必须引用证据，可附 counter_evidence_refs）
# 5) 门评审：POST /v1/stage-gates 冻结
# 6) 日后反查：GET /v1/candidates/C1/trace
```

## 存储与复现

- 证据目录默认 `data/`（已在 `.gitignore`），单文件 `events.jsonl`，每行一条事件。
- 复现方式：用同一数据目录启动服务即自动重放重建全部投影；
  或直接按事件 `hash`/`event_id` 回放。模型由代码、参数、训练数据三个 SHA-256
  哈希唯一定位，训练数据再经数据声明的 `dataset_hash` 锚定。
- 阶段门冻结后即使链尾继续追加事件，冻结材料与冻结头仍可独立校验通过。

## 测试

```bash
npm test        # 等价于 python3 -m unittest -v service_contract test_evidence
```

覆盖：一次入账（含并发重试与重启）、幂等键冲突、追加更正链、许可收紧阻断新计算但
保留旧结论、许可到期、跨方结构脱敏（反查与冻结包）、分叉与有证据合并、判断证据
要求、阶段门冻结/校验/事后追加、候选物全链路反查、哈希链篡改检测、HTTP 契约与身份头。
