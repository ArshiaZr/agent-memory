# 原生批量摄入与评测完成门禁

## 元信息

| 项 | 值 |
|---|---|
| 日期 | 2026-11-10 |
| 影响范围 | evaluation/ingestion、adapter profile helper、S11 |

## 背景

此前 no-source / oracle 诊断不能等同完整原生记忆评测。需要先提交全部源上下文，
走默认提炼/更新/索引，再开始所有查询，同时支持大文件、有限预算与跨进程恢复。

## 决策

实验层增加 Python API / CLI，调用已鉴权的公共 MemoryAPI.add(infer=True)。
复用现有 agent profile，不另造 extractor。流式来源先分页落 SQLite 并 seal；
逐条按作用域内时间顺序写入，保存来源到派生 ID 的映射。所有 seal 的输入成功
返回才能放行 benchmark callback，支持多个 job 的全局门禁。

保留完整来源与原始时间，不读取 answer annotations 作选择；上游输入若本身是
oracle，导入器不会凭空补齐缺失背景，因此完整评测必须显式选 full-history 数据。
成功空提炼单独计数，不能将其自动解释为“没有应记事实”。

## 拒绝的方案

- 不在生产 API handler 编排全数据集，不增加重复的认证入口或 HTTP 路由。
- 不 monkeypatch no-source，也不注入 gold memories。
- 不声称 exactly-once：native write 与 journal 无共同事务，任何不确定写入均阻断。
- 不仅靠增大 top-k，也不拿准备完成当作提炼成功。
- 不改写 t_valid 为源时间；保持原生时钟语义，实验报告需披露此限制。

## 验证

确定性单测覆盖分页幂等与冲突、事务回滚、时间顺序、完整源导入、QA 隔离、
暂停/恢复、并发写入排斥、崩溃/超时阻断、指纹变化和 callback 门禁。
集成测试使用真实 agent engine、agent_llm、shadow dedup、SQLite、文档索引，
只固定模型输出/embedding；跨 runtime 重启再执行已门禁的召回，验证源引用与时间。

## 已知遗留

不承诺模型质量、全基准成绩、跨机器分布式调度、自动费用估计或 HTTP 上传服务。
默认 factory 将原文 KV 改为持久 SQLite 并关闭 watcher，明确披露实验装配差异。
旧 LongMemEval runner 保留为独立诊断，不能绕过新门禁后声称符合完整协议。
原生有限 recent source 缓存不变；journal 保留全量输入但不是自动可检索的源归档。

## 上游同步适配（2026-11-10）

原生 agent profile 上游新增可选的具名 KV 存储。摄入 composition root 只覆盖
kv_store.default，不再整体替换命名空间，保留其他具名依赖。回归检查覆盖可选
store 仍存在、模型/凭证变化指纹，以及 native 提炼/文档存储重启后的门禁召回。
Python 3.13.18 下相关单测与集成测试通过；模型输出为确定性夹具，不调用付费模型。
