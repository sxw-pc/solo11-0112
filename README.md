# 回填补偿编排台

完全离线，输入与主体均为合成数据。`python -m pip install . --no-build-isolation` 安装；`python -m backfillcomp demo` 一条命令初始化 SQLite、导入 fixture、计算、生成 JSON/Markdown；`python -m pytest -q` 测试。

数据流：SourceFile 原始字节及哈希 → RawEvent → Subject 与 RuleVersion → Run 内的 Decision、Evidence、Issue → 报告；身份冲突和坏行保留为 Issue。相同文件哈希与事件键幂等。

状态：待接收→已校验→已判定/待核；决策和证据与运行状态事务提交。错误码包括 parse_error、identity_conflict、evidence_gap、rule_conflict、execution_failure。只操作显式沙箱。

首版补偿能力见 `rules.py`。容量租约与发布核对已加入运行路径。

## 容量租约与消费方发布

`window --db DB --id W --starts 2020-01-01T00:00:00Z --ends 2099-01-01T00:00:00Z --units 2` 定义容量；`reserve --db DB --id L --plan PLAN.json --window W --worker worker-A --units 2` 持久预留。竞争者进入 `waiting`，`activate` 核验计划版本，`apply --lease L` 消费活动租约，完成后释放并提升候选。只要配置了容量窗口，未获活动租约的补偿不能执行。

`publication-check --db DB --target TARGET.db --id G --consumer consumer-A --expected fixtures/consumer-expected.json` 把消费者的期望结果和目标值、目标审计逐行核对，产生 `ready`、`blocked` 或 `uncertain`；`publish` 在提交时再次核验目标和最新运行。目标行被改写使已准备的发布变为 `stale`。`fixtures/scheduling-cases.json` 包含竞争、旧计划和部分执行场景。容量与发布各有独立持久化记录，且执行状态直接影响发布结论。

## 补偿与回退证据

`plan` 保存最新决策运行对应的计划摘要。`apply` 只接受该运行的未改动计划，先核目标行的业务原值与计算值，再在目标 SQLite 同一事务中写账本与 `backfill_audit`。重复执行需要目标审计证明；仅仅看到目标值碰巧等于新值不算已执行。`rollback` 必须找到同一计划的已应用目标审计记录，拒绝未执行计划的回退或被后续改写的目标；重复回退不会再次改账。`verify --sandbox` 同时读取目标值与目标审计，没有审计而值相等时判不确定。控制库与目标库是两个 SQLite 文件，跨库事务原子性尚未实现；异常中断后应先运行 `verify`，不能仅依赖控制库的执行记录。

每条补偿须有对应的规则修订来源事件；缺失或歧义时该行及依赖它的下游行进入待核，不能生成可执行计划。Evidence 绑定来源事件、文件与规则版本；`verify` 会从原始行重新核对事件和摘要。

## 分摊合同与尾差

配置 `allocations` 为受影响的父→子关系声明分摊合同：冻结权重、来源修订事件与有效分区。`contribution: once` 的来源修订差额按权重守恒分摊到直达接收方，同一来源沿多条路径抵达汇总行时按合同授权的来源身份只计一次（`each` 则授权按边多次计入）。尾差合同在计划冻结前确定：`remainder: priority` 把不足一分的尾差分给最高业务优先级接收方，`remainder: carryover` 把尾差保留为独立待结转余额；两路均精确守恒到分，但逐行新值与当轮可补偿集合不同。权重缺失、负数、总权重不符、分区不符或环都进入待核，不生成可执行计划。`contracts --db DB` 查看当前运行的冻结合同、逐边份额与待结转余额；`report` 与 `verify` 沿来源事件和分摊边独立重算每条贡献与尾差并对账，不信既有汇总值。

