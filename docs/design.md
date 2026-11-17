# 业务模型与边界

历史账本的旧值重演、新规则差异、依赖重算与乐观写入共同决定补偿是否安全；失败时目标事务回滚。

## 实体关系

Batch 关联多个 Subject 与 SourceFile；SourceFile 的原始字节和 SHA-256 不覆盖，RawEvent 引用来源行；RuleVersion 被 Evidence 引用；一次 Run 关联多个 Decision、Evidence、Issue。非法输入留下 Issue，不写出无依据的成功结论。

## 状态和失败

导入与身份校验结束后 Run 进入已校验；有无法判定主体时进入待核，否则已判定。规则版本变化产生新 Run，旧 Decision 保留。源哈希冲突、身份冲突、依赖环、证据缺口和执行失败各有独立错误或问题码。写操作只允许显式沙箱，原始来源保持只读。

## 分摊合同

配置可声明 `allocations`：每个合同以上游修订主体为来源，冻结来源修订事件（event_key 与 correction 事件对应）、`contribution`（once/each）、`remainder`（priority/carryover）以及逐条父→子分摊边的权重、业务优先级与有效分区。once 合同要求权重非负且总和恰为 1；来源修订差额按权重下取整到分，尾差或归入最高业务优先级接收方，或记为独立待结转余额（carryover_balance），两路均精确守恒到分。下游汇总行沿依赖边继承带来源身份（事件+分摊边）的贡献条目，同一来源身份沿多条路径抵达时只计一次；each 合同则授权按边多次计入。权重缺失、负数、总权重不符、分区不符、事件不符或环都使来源与接收方进入待核，不生成可执行计划。合同、分摊份额与待结转余额随 Run 持久化（allocation_contract/allocation_share/carryover_balance），计划项携带来源摘要；apply/rollback 只使用冻结计划中的数值与来源摘要，已执行计划不被新权重改写。report 的 export/verify 沿来源事件与分摊边独立重算每条贡献与尾差，与持久化状态对账。

## 首版以外的自然扩展

多数据库补偿、流批并行水位、分区灰度、检查点续做、成本限流、多版本迁移、缓存失效通知、审批签章。以上均未实现。
