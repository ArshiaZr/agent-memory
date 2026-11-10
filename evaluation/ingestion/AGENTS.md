# evaluation/ingestion

## 模块地图

- records.py: 白名单源记录、UTC 时间、稳定序列化与大小限制。
- writer.py: 注入可信身份后调用公共 MemoryAPI.add(infer=True)，不接触私有引擎。
- service.py: SQLite 作业、原子分页、时间顺序、检查点、故障恢复与 benchmark 门禁。
- importers.py: JSONL / 完整 LongMemEval 流式导入；无 oracle session 选择。
- runtime.py: 本地实验 composition root，复用 adapter 原生 agent profile。
- __main__.py / README.md: CLI、明确的实验协议与运行方法。

## 行为铁律

先完整上传并 seal，再执行 native write，所有作业 complete 后才可运行 benchmark。
原生写入与 journal 非原子；异常/崩溃不自动重放，uncertain 必须人工复核。
不把问答标注交给提炼，不覆写内核 clock，不用 raw fact 插入替代原生构建。
checkpoint 只证明 API 返回，不能证明抽取正确或其他进程未改存储。

## 与其他子目录的边界

仅通过 jiuwen_memory.api 写入；默认配置由 adapter 的公共 helper 提供。
不改变生产 HTTP/auth 路径，不使用旧 no_source_extraction monkeypatch。
完整基准的 QA/judge 在门禁之后，保持单独配置与结果归档。
