# 回填补偿编排台

完全离线，输入与主体均为合成数据。`python -m pip install . --no-build-isolation` 安装；`python -m backfillcomp demo` 一条命令初始化 SQLite、导入 fixture、计算、生成 JSON/Markdown；`python -m pytest -q` 测试。

数据流：SourceFile 原始字节及哈希 → RawEvent → Subject 与 RuleVersion → Run 内的 Decision、Evidence、Issue → 报告；身份冲突和坏行保留为 Issue。相同文件哈希与事件键幂等。

状态：待接收→已校验→已判定/待核；决策和证据与运行状态事务提交。错误码包括 parse_error、identity_conflict、evidence_gap、rule_conflict、execution_failure。只操作显式沙箱。

首版补偿能力见 `rules.py`。容量租约与发布核对已加入运行路径。

## 容量租约与消费方发布

`window --db DB --id W --starts 2020-01-01T00:00:00Z --ends 2099-01-01T00:00:00Z --units 2` 定义容量；`reserve --db DB --id L --plan PLAN.json --window W --worker worker-A --units 2` 持久预留。竞争者进入 `waiting`，`activate` 核验计划版本，`apply --lease L` 消费活动租约，完成后释放并提升候选。只要配置了容量窗口，未获活动租约的补偿不能执行。

`publication-check --db DB --target TARGET.db --id G --consumer consumer-A --expected fixtures/consumer-expected.json` 把消费者的期望结果和目标值、目标审计逐行核对，产生 `ready`、`blocked` 或 `uncertain`；`publish` 在提交时再次核验目标和最新运行。目标行被改写使已准备的发布变为 `stale`。`fixtures/scheduling-cases.json` 包含竞争、旧计划和部分执行场景。容量与发布各有独立持久化记录，且执行状态直接影响发布结论。

### 共享执行池

多个实际共用一组 worker 的时间窗口可用 `--pool` 归入同一执行池：

```
window --db DB --id W1 --starts 2020-01-01T00:00:00Z --ends 2099-01-01T00:00:00Z \
       --units 3 --pool workers --pool-units 3 --policy fit_backfill
window --db DB --id W2 --starts 2020-06-01T00:00:00Z --ends 2099-06-01T00:00:00Z \
       --units 3 --pool workers
```

`units` 仍是该窗口的局部上界；`--pool-units` 是同一时刻所有重叠窗口中 `reserved` 与
`active` 租约消耗的共同上界。首尾相接（`ends == starts`）不算重叠。池容量和提升策略
在池首次建立时固定，后续声明必须一致；更新已有窗口若会让已持有租约越界或落入已关闭
的时间区间，整笔拒绝。未设置 `--pool` 的旧窗口保持原有的单窗口语义，旧数据库在打开时
自动补齐队列和审计列。

`reserve`、`activate`、`release` 都在一个 SQLite 写事务内同时核对窗口与池两个上界，
因此跨窗口并发申请只有一个赢家。租约绑定提出时的计划；计划变旧后 `activate` 将其置为
`blocked`，换窗口重新申请是身份冲突，不能复活。

等待队列按持久化的 `wait_seq` 处理：

- `strict_fifo`：队首放不下时，其后所有租约继续等待。
- `fit_backfill`：可跳过暂时放不下的队首，提升能放入剩余额度的小租约；被跳过的租约保留
  原队列位置与 `skipped_count`，之后容量足够时先于新请求获得提升。两种策略在释放后都
  优先处理队首，连续小租约不会永久饿死它。

`capacity-status --db DB [--pool P]` 查看池占用、窗口占用、等待位置与跳过次数；
`capacity-audit --db DB [--pool P] [--lease L]` 查询每次申请、提升、跳过、激活、释放
及中断恢复的持久审计；`recover --db DB` 在外部中断后校正越界状态并报告剩余问题。

## 补偿与回退证据

`plan` 保存最新决策运行对应的计划摘要。`apply` 只接受该运行的未改动计划，先核目标行的业务原值与计算值，再在目标 SQLite 同一事务中写账本与 `backfill_audit`。重复执行需要目标审计证明；仅仅看到目标值碰巧等于新值不算已执行。`rollback` 必须找到同一计划的已应用目标审计记录，拒绝未执行计划的回退或被后续改写的目标；重复回退不会再次改账。`verify --sandbox` 同时读取目标值与目标审计，没有审计而值相等时判不确定。控制库与目标库是两个 SQLite 文件，跨库事务原子性尚未实现；异常中断后应先运行 `verify`，不能仅依赖控制库的执行记录。

每条补偿须有对应的规则修订来源事件；缺失或歧义时该行及依赖它的下游行进入待核，不能生成可执行计划。Evidence 绑定来源事件、文件与规则版本；`verify` 会从原始行重新核对事件和摘要。

