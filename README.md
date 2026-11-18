# 回填补偿编排台

合成回填计算作业的共享计量表可用 `energy-record` 登记带复位证明的读数、活跃作业集合、联合作业校准和逐槽电网强度；`energy-summary` 仅恢复逐槽总 Wh 与总排放量。示例：`python -m backfillcomp init --db ledger.db --config fixtures/config.json`，然后 `python -m backfillcomp energy-record --db ledger.db --input fixtures/energy-meter.json`，最后 `python -m backfillcomp energy-summary --db ledger.db --id meter-S1`。本地合成计量不会访问真实电表。

## 逐作业能源与排放归属

`energy-attribute --db DB --id meter-S1 --contract direct_platform_overhead` 在冻结来源版本上按相邻边界读数重建每个半开时槽的非负 Wh（计数器复位须凭该边界 old/new 的独立证明衔接，读数回落不算负耗能、不猜测复位次数），再按事先批准的归属合同把并发耗能分给各作业与平台。`direct_platform_overhead` 由平台承担空闲校准 E(∅) 与非线性共享剩余（剩余为负则待核，不硬造负平台耗能），每个作业只承担单独增量；`shapley_shared_overhead` 平台只承担 E(∅)，作业对 v(S)=E(S)−E(∅) 按全部子联盟的精确 Shapley 边际贡献分摊（最多四个同时活跃作业，使用阶乘权重）。校准须含空集与活跃集合的全部子集、随集合扩大不下降、且与观测槽 Wh 一致；缺校准、复位身份冲突、计量断点、活跃集不匹配或强度缺失一律待核，不分摊猜测数字。每槽以精确有理数 Wh 按该槽 gCO2e/kWh 换算，各作业与平台每槽及全期之和严格等于计量总 Wh/g。归属账写盘后不可回写：同 source_id 同字节重放幂等、异文冲突，新读数或校准只生成新归属版本。`energy-verify --db DB` 从冻结原始读数、复位事件、活跃集合、联盟校准和强度独立复算所有 Wh/g，发现漏掉复位、交换作业份额、篡改平台剩余或用错槽强度。

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

