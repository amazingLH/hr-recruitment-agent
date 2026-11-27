# 评测协议与结果汇总

本模块固定评测范围、记录结构和统计口径。评测范围是输入配置，测量结果由逐条观察记录计算；本模块不内置准确率、F1或模型评分常量。

## 数据来源与执行边界

汇总器只读取带原始证据文件引用的JSONL记录，不调用模型、不向业务接口写入，也不发送邮件。每次报告保存输入文件、协议和证据文件的SHA-256指纹。源记录仍需来自实际评测执行，文件存在与哈希一致不能单独证明记录内容正确。

`scope_complete`只表示记录数量与范围配置匹配，不代表所有任务通过。零分母输出null；缺少原始证据、重复ID或不合法分数会终止汇总。使用`--strict-scope`要求完整覆盖配置范围。

## 运行

在仓库根目录使用Python 3.11及以上版本运行。仅使用标准库，不依赖数据库或模型凭据。

```bash
python benchmarks/evaluation/aggregate_metrics.py --project hr --records PATH_TO_RECORDS --output PATH_TO_REPORT
python -m unittest discover -s benchmarks/evaluation -p "test_*.py" -v
```

Copilot另外传入`--inventory`指定与本次实验一致的工具Schema目录。DeepResearch工程回归传入`--junit`指定同次pytest执行生成的JUnit XML；计数由唯一测试ID与执行状态计算。

## 所有记录共同字段

| 字段 | 含义 |
| --- | --- |
| record_id | 全局唯一的观察记录编号 |
| kind | 观察类型，见各项目说明 |
| case_id | 固定测试用例或引用核验单元编号 |
| source_artifact | 相对JSONL文件的原始请求/响应、工具轨迹或审计文件路径；文件必须存在且非空 |

将原始记录和生成的结果存放在受控目录，公开前清理凭据、候选人资料、邮件和内部知识正文。仓库不要求上传私有实验材料。

## HR评测范围

| 项目 | 协议数量 | 计数单位 |
| --- | --- | --- |
| Agent用例 | 150 | 独立业务场景 |
| 每例重复 | 3 | 固定配置下的独立运行，完整范围共450次 |
| 知识问答 | 120 | 带标准证据的问题 |

业务分层数量见`protocol.json`，覆盖JD、编辑、简历、面试、试卷、邮件、知识问答及资源边界。

### Agent记录

`kind=agent`，附带`repeat_index`（1至3）、`expected_intent`、`actual_intent`、`expected_confirmation`、`actual_confirmation`和非空的`contract_checks`布尔检查表。意图与确认按每次运行比较；输出契约必须全部必要检查成立才通过。每项输出保留分子、分母与比例。

确认判断准确率衡量是否需要确认的决策，不等于高风险动作审批覆盖率。后者还需检查执行前状态和实际审批事件。

### RAG记录

`kind=rag`，附带`gold_evidence_ids`与按名次排列的`retrieved_ids`。Recall@5先对每题求前5证据与标准集合的交集比例，再按题平均；Hit@5只判断是否至少命中一条。文档级和块级ID不能混用。

如记录`ragas_answer_correctness`，必须同时提供`ragas_version`和`judge_model`。得分范围0至1；缺测保留缺测数量，不补常数。对照实验分别记录向量、全文、融合及重排配置；当前业务实现使用加权融合。
