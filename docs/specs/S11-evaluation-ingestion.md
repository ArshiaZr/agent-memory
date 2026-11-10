# 评测前原生摄入契约

## 元信息

| 项 | 值 |
|---|---|
| 关联模块 | evaluation/ingestion、jiuwen_memory_adapter/jiuwenswarm |
| 最近一次修订日期 | 2026-11-10 |
| 关联特性文档 | evaluation/F01-native-bulk-ingestion |

## 范围 / 边界

实验 Python API / CLI；不扩展生产 HTTP，不绕过 S02 的可信安全上下文。
公共 agent_default_config() 返回 adapter 相同原生 agent 配置的独立副本，供可信装配使用。

## 不变量

- SourceRecord 只含 ID、文本、原生 Scope、带时区 observed_at、project/source_session。
  问答标注不能进入 writer。每条最多 2 MiB，单次 append 最多 100 条 / 8 MiB。
- create → append → seal → run → complete → run_benchmark。
  append 原子、同偏移同内容可重试，来源 ID 唯一，作用域内时间单调不减；seal 后不可改。
- writer 使用注入安全上下文调用 add，infer=True，源时间放 occurred_at/observation_date。
  原生提炼、去重、更新、构建的返回是检查点边界；不覆写 validity 时钟。
- upload/seal 不启动模型提炼。run 同一 job 只允许一个持有 token 的 worker；按顺序写。
- native write 与 journal 的事务不共享；运行异常/进程丢失标 uncertain，禁止自动重放。
  recover 必须确认原 worker 已停止，retry 必须明确承认部分写入风险。
- 回执须来自 journal，全部 sealed records succeeded，writer/config/code/storage 指纹匹配；
  所有 job 通过后才调用 benchmark，不能仅信调用者提供的 JSON success 标志。
- 回执不证明语义正确、外部存储未变或长期源引用仍可解析。空提炼必须显式计数。

## 接口契约

`IngestionService(journal, writer)` 的公共面：
`create(dataset_id)`、`append(job, records, offset=...)`、`seal(job)`、
`run(job, max_records=None)`、`status(job)`、`results(job, offset=0, limit=100)`、
`recover(job, worker_stopped=False)`、`retry_uncertain(job, acknowledge_partial_write=False)`、
`require_ready(job)`、`run_benchmark(job_ids, callback)`。

writer: `fingerprint: str`, `write(SourceRecord) -> list[str]`。
`MemoryAPIWriter` 注入 public API、安全上下文 provider、有效 pipeline_spec；
spec 必含 revision、storage_id 和 config。部署方不得用数据集 target 派生可信身份。
默认本地 composition root 显式使用固定开发 ROOT；生产消费方须注入其真实鉴权上下文。

## 数据结构

job 状态 uploading / sealed / running / blocked / complete；record 状态
pending / running / uncertain / succeeded。记录源摘要、scope、时间、派生 IDs；
错误诊断只存类型，不存可能含凭证的 provider 原始错误。输入文本完整保存在本地 journal，
因此 journal 应和 benchmark 原数据一样管理。返回 unit 数是逐调用 ID 数总和，不是最终唯一数。

## 与其它 spec 的关系

沿用 S01 输入、S02 MemoryAPI、S05 构建与 S06 存储契约；无修改其安全/业务语义。
默认 factory 只为离线重启将原文 KV 持久化、固定隔离存储路径并关闭 watcher。
