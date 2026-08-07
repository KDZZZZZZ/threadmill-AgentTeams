# Threadmill 领域语言

Threadmill 协调软件工作，但不把工作绑定在执行它的 agent session 上。仓库中的设计文档、接口和 Skill 应统一使用下面这些词。

## 工作

**Requirement（需求）**:
对目标、动机和约束的原始表达。Requirement 用来保留来源，不是可直接调度的工作。
_Avoid_: Request、prompt、ticket

**Task Contract（任务契约）**:
一个工作单元的稳定约定，包括要改变什么、为什么、允许的边界和验收条件。
_Avoid_: Task prompt、plan

**Task（任务）**:
由一个 Task Contract 约束、通过 phase endpoint 协调的持久工作单元。Task 的寿命长于任何参与执行的 agent。
_Avoid_: Agent、session、thread

**Task Attempt（任务尝试）**:
对同一个 Task Contract 的一次有界尝试。验证失败后通常开始新的 attempt，而不是创建新的 task。
_Avoid_: Retry task

**Phase Endpoint（阶段端点）**:
Task 生命周期中的命名协调点，其他工作可以向它提供输入或依赖它产生的结果。
_Avoid_: Status flag、agent state

**Coordination Graph（协调图）**:
由 task、phase endpoint、依赖、blocker 和 decision 构成的持久图。它记录不同工作之间尚未履行的因果义务。
_Avoid_: Workflow diagram、agent chat graph

**Execution Graph（执行图）**:
为执行某个 phase 或 attempt 临时物化的图；节点可以调用 agent、tool 或另一个 execution graph。
_Avoid_: Task Graph

## 运行

**Agent Invocation（Agent 调用）**:
在明确角色、输入、权限、预算和输出约束下对 agent 的一次有界使用。它是可替换的计算资源，不是持久项目身份。
_Avoid_: Agent task

**Thread（会话线程）**:
某个 provider 为一次 Agent Invocation 保留的局部对话状态。丢弃 Thread 不应丢失 Task 或已经接受的项目事实。
_Avoid_: Task、project memory

**Worker Capacity（工作容量）**:
Scheduler 当前可以并发使用的 Agent Invocation 数量。容量只改变吞吐，不改变 Coordination Graph 的含义。
_Avoid_: Agent assignment

## 证据和上下文

**Evidence（证据）**:
用于判断某项主张的可观察结果，例如 diff、测试结果、tool output 或人工决定，并且可以追溯来源。
_Avoid_: Agent summary

**Project Fact（项目事实）**:
在相应验收或决策边界通过后，获准供后续工作复用的主张。
_Avoid_: Memory、note

**Context Block（上下文块）**:
从 event 或 artifact 中提炼出的、可追溯且可复用的陈述。Context Block 可能被替代，不能自动视为 Project Fact。
_Avoid_: Chat excerpt

**Context Pack（上下文包）**:
针对一次 Agent Invocation 及其精确工作边界选出的有限 Context Block 快照。
_Avoid_: Full project history、prompt dump
