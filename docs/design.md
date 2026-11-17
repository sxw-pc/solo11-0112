# 业务模型与边界

历史账本的旧值重演、新规则差异、依赖重算与乐观写入共同决定补偿是否安全；失败时目标事务回滚。

## 实体关系

Batch 关联多个 Subject 与 SourceFile；SourceFile 的原始字节和 SHA-256 不覆盖，RawEvent 引用来源行；RuleVersion 被 Evidence 引用；一次 Run 关联多个 Decision、Evidence、Issue。非法输入留下 Issue，不写出无依据的成功结论。

## 状态和失败

导入与身份校验结束后 Run 进入已校验；有无法判定主体时进入待核，否则已判定。规则版本变化产生新 Run，旧 Decision 保留。源哈希冲突、身份冲突、依赖环、证据缺口和执行失败各有独立错误或问题码。写操作只允许显式沙箱，原始来源保持只读。

## 执行容量池

`capacity_window` 保存半开时间区间 `[starts, ends)` 与局部容量 `units`；`capacity_pool`
把重叠窗口绑定到同一物理执行池，`capacity_pool.units` 是任一瞬间这些窗口中 `reserved`
与 `active` 租约之和的上界。池占用按区间端点求最大同时占用，首尾相接的窗口不重叠。

`reserve`、`activate`、`release` 在同一个 `BEGIN IMMEDIATE` 写事务中读取并更新租约，
跨窗口竞争由 SQLite 串行化。`execution_lease.wait_seq` 持久化排队顺序，`skipped_count`
记录被跳过的次数。`strict_fifo` 在队首放不下时停止提升；`fit_backfill` 可跳过队首提升
可放入的小租约，但从不改写 `wait_seq`，因此被跳过者恢复容量后仍先于新请求。池的策略在
建立时固定。

租约绑定 `plan_digest`、`run_id` 与计划 `target_hash`；计划变旧或目标哈希变化时
`activate`/`apply` 将其置为 `blocked`，且换窗口重新申请属于身份冲突而不是复活路径。
`check_active_lease` 在 `apply` 前重新核对活动状态、计划有效性与池/窗口两个容量上界。

`capacity_audit` 逐条记录申请、入队、提升、跳过、激活、释放、阻断与恢复，附带当时
窗口/池占用与等待位置。所有状态变更都是单事务，进程中断不会留下半提交状态；`recover`
仅在检测到越界时按逆队列顺序把最新租约退回 `waiting`，不自动提升、不跨窗口移动。

## 首版以外的自然扩展

多数据库补偿、流批并行水位、分区灰度、检查点续做、成本限流、多版本迁移、缓存失效通知、审批签章。以上均未实现。
