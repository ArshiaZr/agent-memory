# F09 — 关键词、语义召回与精排的可靠性

## 元信息

| 项 | 值 |
|---|---|
| 日期 | 2026-11-10 |
| 范围 | QueryParser、PipelineRetriever、Reranker capability、Routing、shadow 有效期、Swarm adapter project |
| 规约 | S04-retrieval、S07-common |

## 问题与修复

1. 默认 overlap 用词重叠分替换融合分，可把无共同词的语义正确候选置零，也会覆盖已有
   BM25_scored_fusor 的加项。新增 `supports_semantic_ranking` 能力：自动精排只使用声明
   支持语义排序的 scorer；overlap 默认跳过，显式 `rerank=True` 保留兼容实验入口。
   跳过保留融合分、顺序、evidence，走未精排阈值；轨迹明确记录原因。
2. BGE/API 模型仍可自动精排；Routing 透传当前 active scorer 的声明。错误数量、NaN、
   非数值等输出不能重写有效候选，按后端失败降级保留融合结果。
3. query embedding 的 BackendError 不应让健康的关键词索引无法使用。解析阶段记录
   VECTOR/query_embedder 通道错误，继续词法召回；ValidationError 等调用错误不吞掉。
4. 当前态有效期只在点读后检查，以及 shadow 丢掉历史起点 NOT，使未来/失效候选占满
   有限池。当前态以同一 UTC 时钟下推和复核 `[t_valid,t_invalid)`；shadow 只安全编译
   完整的起点否定，不扩大到对未知子树做部分取反。
5. prefetch 透传写入侧 project，并与提炼 tier 条件 AND；显式检索未给 project 的描述
   改为共享记忆，与既有隔离口径一致，不把遗漏 project 变成跨项目全库查询。

## 决策与拒绝方案

- 复用已有 BGE/API 插件，不强制远程模型；离线部署仍须可用。
- 不把 RRF 原始分与模型分直接相加：量纲不一致，任意常数不能代表校准。
- 不靠扩大 top-k 修有效期，也不靠放宽 project 换覆盖率。
- 不把 hashing 宣称为语义模型；不在已有索引上静默替换模型/维度。
  embedding 模型变化应使用隔离索引并重建，保持写入/查询同一空间。

## 可选语义精排配置

在既有 agent profile 上覆盖 reranker，不覆盖其存储和鉴权配置：

```yaml
globals:
  rerank_enabled: true
reranker:
  default:
    target: bge_reranker
    params:
      reranker_bge_model: BAAI/bge-reranker-v2-m3
      reranker_bge_fp16: false
      reranker_bge_normalize: true
```

本地模型需可用依赖和权重；API 精排使用已有插件。此处不声称离线环境已完成真实模型
质量/时延验证，也不替换出厂 embedding 模型。

## 回归与证据

- 三种 Storage pipeline：语义正确、零词重叠的 espresso 候选在自动路径保留；显式词法
  模式仍可复现置零，确保兼容开关有明确边界。
- 模型 scorer、Routing active 切换、非法模型分数、embedding 后端失败的词法回退。
- 真实 SQLite：60 个未来版本加一个到期版本不再挤掉唯一有效记录；覆盖当前/历史、
  开放起点、半开边界和不能安全下推的 NOT 子树。
- Adapter prefetch 实际调用的 filters 同时含 project 和 tier。
- 已运行 retrieval、SQLite shadow、BGE/API reranker 单测；mock 模型不代表真实质量。

入口：`tests/unit/retrieval/test_semantic_rerank_guard.py`、
`tests/unit/storage/test_shadow_validity_recall.py`、
`tests/unit/jiuwen_memory_adapter/test_agent_memory_provider_unit.py`。

## 后续受控实验

冻结候选池、query、上下文预算，对比 fusion-only / 显式 overlap / BGE / API。
分别报告必要事实覆盖率、MRR/nDCG、答案正确率、候选召回与最终交付损失、p50/p95 时延。
embedding 对比使用独立重建的索引，固定 reranker；keyword-only、vector-only、hybrid
与 leave-one-arm-out 分开归因。普通词抑制、词形变化、中文分词、chunk 长度和强单通道
候选保护先做受控实验，不靠手工改一个例子宣布普遍提高准确率。

## 验证环境与限制

2026-11-10 在本地独立 fork 的 exp 分支使用 Python 3.14、pytest 8.4.2，
执行 retrieval 全目录、SQLite shadow、BGE/API scorer 与 Swarm adapter 定向单测。
这些确定性检查不代表完整基准成绩、真实模型准确率或真实服务时延。
真实 sqlite-vec 扩展虽已安装，相关单测并不等价于完整部署验证。
此提交包含已验证的代码修复，不包含此前研究中发现但尚未实现的抽取质量、
源文本长期归档、回答器推理或模型输出修复方案。

## 上游同步适配（2026-11-10）

合入当日 upstream/mem2.0，保留 staged retriever 的阶段拆分、可选 pipeline、
项目检索与新的默认 agent profile 能力，同时保留语义精排能力护栏、分数校验、
embedding 故障回退和有效期下推。prefetch 夹具补齐上游新增的初始化前置条件，
不移除实际运行时门禁。清除 S03 上游遗留的日期冲突标记。

Python 3.13.18 / pytest 8.4.2 的相关单测与 native ingestion 集成测试共 731 例通过；
范围包含 retrieval 全目录、SQLite shadow、BGE/API scorer、adapter、evaluation、
query pipeline 选择与 control pipeline 解析。相关改动文件 Ruff 检查通过。
该结果不代表 Swarm 完整 SDK、全仓 CI、真实模型质量或新基准分数。
