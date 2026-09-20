# AI药研决策证据链

服务用于串联药物发现中的计算预测、实验观察与阶段决策，保留合作方之间必要的保密边界。

项目当前提供稳定的基础服务入口，便于本地联调和运维巡检。运行 `python3 service.py --check` 可核对服务配置；执行 `python3 service.py --port 8000` 后访问 `/health` 可确认服务身份。

## 领域模型

所有事实统一建模为**不可变实体**（`evidence.Entity`），以带类型的引用连成证据图谱：

| 实体类型 | 含义 |
| --- | --- |
| `target_hypothesis` | 靶点假设 |
| `data_declaration` | 训练数据声明 |
| `model_version` | 模型与参数哈希 |
| `candidate_molecule` | 候选分子 |
| `synthesis_batch` | 合成批次 |
| `experiment_result` | 实验结果（原始测量） |
| `judgment` | 人员判断 |
| `derivation` | 一次受政策约束的计算（内嵌法律依据快照） |
| `license_grant` | 许可授予（用途、密级、保密期，可修订） |
| `stage_gate` | 阶段门评审冻结记录 |
| `branch_fork` / `merge` | 探索线分叉 / 合并记录 |

引用方向约定：后产生的实体指向其依据（出边 = “我基于谁”）。常用关系：`trained_on`、`consumes` / `produces`、`uses_model`、`synthesized`、`measured_on`、`supports` / `contradicts`（反证）、`regards`（判断对象）、`tests_hypothesis`、`reviews`、`evidence`、`corrects`。引用目标必须已入账，悬空引用在写入时被拒绝。

## 需求与机制对照

| 业务要求 | 机制（`evidence.py`） | 验证 |
| --- | --- | --- |
| 各类事实彼此引用 | 统一不可变实体 + 引用完整性校验（`ingest`） | `EntityGraphTest` |
| 原始测量只能追加更正，不能覆盖 | 实体为冻结数据类；`correct()` 追加新版本并链接 `corrects`，`history()` 全量取回 | `AppendOnlyCorrectionTest` |
| 合作方重试一次入账 | 幂等键按上传方作用域去重，重试返回首次结果 | `IdempotentIngestTest` |
| 许可/保密期变化阻断新的不合规计算 | `derive()` 发生时逐条校验用途、密级与有效期，失败即拒绝且不落任何记录；许可义务沿推导链向下游继承 | `LicenseEnforcementTest` |
| 当时合法形成的结论不删除 | 推导记录内嵌法律依据快照（许可版本 id + 内容哈希）；许可修订同样走追加式更正，旧版本永久保留 | `LicenseEnforcementTest` |
| 两条探索线可分叉、可合并 | `fork()` / `merge()`；`branch_view()` 按分叉点与合并点精确计算可见集 | `ExplorationLineTest` |
| 阶段门冻结所见材料、利益冲突与批准意见 | `freeze_stage_gate()` 把材料清单逐条钉在内容哈希上；`verify_stage_gate()` 可复核；会后更正不改变当时所见版本 | `StageGateTest` |
| 从候选物反查完整重现 | `traceback()` 沿出边、更正链与解释性入边（谁算出它、谁判断它、什么实验支持/反驳它、哪个阶段门评审过它）求闭包 | `TracebackTest` |
| 不向无权合作方泄露他方化合物结构 | 密级隔舱 + 查看方上下文：越权实体以打码占位符返回，载荷与引用不外泄 | `ConfidentialityBoundaryTest` |

## HTTP API 摘要

`service.py` 只做 JSON 编解码与错误映射（领域错误 → 400，实体缺失 → 404，政策拒绝 → 403）。

| 方法与路径 | 说明 |
| --- | --- |
| `GET /health` | 服务身份与健康检查 |
| `POST /entities` | 登记实体（支持 `idempotency_key`） |
| `GET /entities/{id}` | 读取实体（`?compartments=a,b` 指定查看方密级） |
| `POST /entities/{id}/corrections` | 追加式更正 |
| `GET /entities/{id}/history` | 更正链全量历史 |
| `POST /licenses` / `POST /licenses/{id}/amendments` | 许可授予 / 修订 |
| `POST /derivations` | 记录一次计算（许可校验失败返回 403） |
| `POST /branches/fork` / `POST /branches/merge` | 探索线分叉 / 合并 |
| `GET /branches/{name}/view` | 探索线可见实体集 |
| `POST /stage-gates` / `GET /stage-gates/{id}` | 冻结评审 / 复核冻结清单 |
| `GET /traceback/{id}` | 反查重现（支持查看方密级打码） |

## 运行与测试

```bash
python3 service.py --check     # 基础检查
python3 service.py --port 8000 # 启动服务
npm test                       # 全部测试（等价于 python3 -m unittest -v service_contract test_evidence）
```

## 边界与后续

* 当前为内存存储，便于联调；实体不可变、内容哈希与追加式语义与持久化事件存储（append-only log）一一对应，替换存储层不影响领域规则。
* 查看方密级目前由调用方显式传入（`?compartments=`），接入统一鉴权后应由身份令牌推导，不再信任请求参数。
* 打码占位符保留实体 id 与类型（图形状可见、内容不可见）；若连“存在性”也需隐藏，可在 `_public` 中改为整体剔除并裁剪边。
