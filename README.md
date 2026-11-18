# 回填补偿编排台

可用 `causality-pair` 冻结两个真实判定 run 的主体状态、证据事件 ID/payload/digest、来源原始字节摘要以及配置与账本快照身份；`causality-manual` 只列翻转主体与新增事件 ID，不判断原因。新增 `causality-assess` / `causality-verify` 在冻结事实之上出具事件/来源级历史责任证书：以来源文件为最小干预单元，对全部新增来源子集做 2^n 沙箱真实回放（临时控制库内 `ingest.load`→`rules.evaluate`），产出两种回答不同问题的因果合同。

## 因果诊断证书

两种合同共享同一份全量冻结事实，均不改动任何来源、旧 run、目标账本、补偿金额、容量租约或消费者发布：

- `necessary_change`（单独必要原因）：列出在完整比较运行中**单独撤去**后即使目标主体回到基线状态的新增来源；不存在时结论显式为 `none` 并升级复核，绝不任选一个。
- `minimal_sufficient_change`（最小充分原因组合）：列出**所有**纳入基线后已足以复现比较运行目标状态、且任何真子集都不足以致因的最小新增来源集合；不会只报词典序第一个。同一来源文件的多条事件始终作为一个单元整体纳入/排除。

合成示例：原有 init/load/evaluate 后分别导入 `fixtures/causality-dup-r1.jsonl` 和 `fixtures/causality-dup-r2.jsonl` 再 evaluate，然后
`python -m backfillcomp causality-pair --db ledger.db --id pair-1 --baseline 1 --comparison 2 --subject r2`，再
`python -m backfillcomp causality-assess --db ledger.db --id pair-1 --contract necessary_change`（结论 `none`，升级复核）与
`... --contract minimal_sufficient_change`（同时列出 `{dup-r1}` 与 `{dup-r2}` 两条独立最小充分原因；dup-r1 经 r2→r1 父依赖同样使 r2 待核，调查清单要求分别调查两条责任链）。

证书按 `pair_id`、合同与冻结来源摘要形成不可变新版本：同字节重放幂等（跨进程 digest 一致），事实变化后再 assess 抛 `causality_certificate_conflict`。`causality-verify` 独立从 `source_file`/`raw_event`/`evidence`、配置与账本快照摘要复算 gate、在沙箱重放每一条反事实状态，能检出漏报第二个充分原因、错选子集、伪造来源、篡改历史 run 与遗漏上游传播。

### 可比性闸门与搜索边界

下列情形判 `unsupported`，不得把两个不可比运行的状态差硬称因果：批次不一致、配置/规则版本/账本快照身份漂移或缺失、基线事件消失、旧来源内容变异、来源摘要或原始字节不符、比较事件无法解析到冻结事件、任一 run 的来源链未通过 `report.verify`。同 `event_key` 异文、无法归属的 `identity_conflict`/解析失败或原始字节缺失判 `indeterminate`（保留不确定，不把失败当正常“待核”）；来源跨版本不混合。新增来源单元 >12、主体 >12 或依赖边 >30 时只输出 `indeterminate`，约束内则完整枚举 2^12 并在证书中报告搜索完整性（subsets_total/evaluated/complete）。

`fixtures/causality-cases/`（由 `tools/build_causality_fixtures.py` 生成）覆盖：一份文件两条事件的原子单元、补齐缺证的改善变化与重复修订恶化变化共同出现的非单调场景、同 event_key 异文、循环依赖、缺失原始字节与 13 主体规模上限。

完全离线，输入与主体均为合成数据。`python -m pip install . --no-build-isolation` 安装；`python -m backfillcomp demo` 一条命令初始化 SQLite、导入 fixture、计算、生成 JSON/Markdown 并出具两份因果证书；`python -m pytest -q` 测试。

数据流：SourceFile 原始字节及哈希 → RawEvent → Subject 与 RuleVersion → Run 内的 Decision、Evidence、Issue → 报告；身份冲突和坏行保留为 Issue。相同文件哈希与事件键幂等。

状态：待接收→已校验→已判定/待核；决策和证据与运行状态事务提交。错误码包括 parse_error、identity_conflict、evidence_gap、rule_conflict、execution_failure。只操作显式沙箱。

首版补偿能力见 `rules.py`。容量租约与发布核对已加入运行路径。

## 容量租约与消费方发布

`window --db DB --id W --starts 2020-01-01T00:00:00Z --ends 2099-01-01T00:00:00Z --units 2` 定义容量；`reserve --db DB --id L --plan PLAN.json --window W --worker worker-A --units 2` 持久预留。竞争者进入 `waiting`，`activate` 核验计划版本，`apply --lease L` 消费活动租约，完成后释放并提升候选。只要配置了容量窗口，未获活动租约的补偿不能执行。

`publication-check --db DB --target TARGET.db --id G --consumer consumer-A --expected fixtures/consumer-expected.json` 把消费者的期望结果和目标值、目标审计逐行核对，产生 `ready`、`blocked` 或 `uncertain`；`publish` 在提交时再次核验目标和最新运行。目标行被改写使已准备的发布变为 `stale`。`fixtures/scheduling-cases.json` 包含竞争、旧计划和部分执行场景。容量与发布各有独立持久化记录，且执行状态直接影响发布结论。

## 补偿与回退证据

`plan` 保存最新决策运行对应的计划摘要。`apply` 只接受该运行的未改动计划，先核目标行的业务原值与计算值，再在目标 SQLite 同一事务中写账本与 `backfill_audit`。重复执行需要目标审计证明；仅仅看到目标值碰巧等于新值不算已执行。`rollback` 必须找到同一计划的已应用目标审计记录，拒绝未执行计划的回退或被后续改写的目标；重复回退不会再次改账。`verify --sandbox` 同时读取目标值与目标审计，没有审计而值相等时判不确定。控制库与目标库是两个 SQLite 文件，跨库事务原子性尚未实现；异常中断后应先运行 `verify`，不能仅依赖控制库的执行记录。

每条补偿须有对应的规则修订来源事件；缺失或歧义时该行及依赖它的下游行进入待核，不能生成可执行计划。Evidence 绑定来源事件、文件与规则版本；`verify` 会从原始行重新核对事件和摘要。

