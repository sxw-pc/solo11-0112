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

## 跨库中断恢复裁定

`apply` 的目标库事务（ledger 与 `backfill_audit`）与控制库事务（`execution`、租约释放）是两个 SQLite 文件中的独立提交。`rules.py` 暴露 `_fault_between_commits` 故障注入点，测试与演示用它精确制造“目标已 applied、控制库无 execution、租约仍 active”的中断现场。

`migrate --db DB` 对既有控制库幂等补齐 `repair_case`、`repair_row` 等新表。`repair-inspect --db DB --sandbox SANDBOX --plan PLAN.json --id CASE [--worker W --lease L]` 把 plan_digest、run_id、逐行 before/after、`backfill_audit`、控制库 `execution`、`execution_lease`、`publication_gate` 冻结为带证据哈希的案例：

- 任一行目标值与审计矛盾、业务原值被第三方改写、仅有新值而无 applied 审计、计划已不是最新 run 的计划、控制库只有部分 execution、租约被其他工作者占用或身份不符、目标库/审计表缺失，均为硬阻断，自动恢复被拒绝并给出原因码。
- 同一未发布、计划未变、目标全部行带 applied 审计且控制库零 execution 的中断现场可恢复，维护者在冻结证据上二选一：`repair-decide --route forward` 补记控制库 execution 并释放原租约，补偿成为已完成，随后 `publication-check` 可判 ready；`repair-decide --route rollback` 按原计划逆序恢复旧值、把审计改为 rolled_back、写入 rollback execution 并释放原租约，补偿成为未应用，发布裁定只能是 uncertain/blocked。
- 两条路线都重新验证旧哈希、目标审计、计划摘要和租约身份，不能只凭目标值相等选择。存在已发布消费者时两条路线均禁止（回退给出 `rollback_forbidden_published`），且禁止手段不依赖控制库案例状态——决定时刻重新取证，手工改写控制记录无法规避。
- 相同案例、相同路线、相同证据的 `repair-decide` 重复执行幂等；回退路线在目标提交后再次中断时，重入会跳过已 rolled_back 的行并补齐控制侧，不留下半数行与控制库状态。决定失败时两个库各自保持事务前状态。
- 中断后仍 active 的同一租约只能被其工作者消费或释放，其他计划的租约在窗口内保持 waiting，恢复完成后才提升。`repair-report --db DB --out FILE` 输出两侧证据、可选与选定路线、每行最终状态和阻断原因的 JSON/Markdown。
- 目标审计在 applied 与 rolled_back 之间变化（或被补记控制 execution）后，`publication-check` 对非 published 门禁按最新目标快照重新裁定；`published` 门禁永不被静默改写。

