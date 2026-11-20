# 离线作业编排：依赖传播、运行授权与成套发布

本文件说明 `orchestrator/` 的实现约定与运行、测试方式。来源资料与外部演练
程序协议见 docs/source-contract.md；本文件只覆盖编排层。

## 总览

```
来源事件（交付/修订） ──▶ 来源事件簿（逐日齐全状态+内容指纹）
                              │
作业声明（jobs.json） ──▶ 分区派生（读取范围 → 期望版本指纹）
                              │
                    运行授权（run_token）→ 外部演练程序 → 回执校验
                              │
                    发布组闭包一致 → 成套版本发布
```

状态落 SQLite（默认 `state/orchestrator.db`），产物与发布集落文件
（默认 `outputs/`）。全部用 Python 3.14 标准库，离线可运行。

## 分区与读取范围

每个作业分区 = （作业， 范围， 分区键）。日分区键为日期，周分区键为周一日期，
月分区键为 `YYYY-MM`。候选分区只对有来源声明覆盖的范围生成。

读取窗口按声明展开，**不按同日分区对齐**：

- 日粒度作业读取当天；服务资格取服务当天的人员维度；
- `demand_week` 读取 `[周一-28天, 周一+7天)`，起点不早于本乡镇有来源记录的
  第一天（记录开始之前不是"未知"）；
- `county_control` 读取全部乡镇同周需求周；
- `commitment_month` 读取与月份相交的承诺有效段、与月份相交的需求周、当月
  服务点日维度；跨月需求周被两个月份共同引用，但全仓只有一份分区。

## 指纹、复用与影响扩展

- 来源输入的版本 = 来源内容指纹（交付指纹，应用过修订则追加 `~修订号`）；
- 作业输入的版本 = 上游期望版本 + 上游当前产物摘要，因此上游任何变化立即
  沿依赖链传导，不需要逐层跑完才发现；
- 分区的期望版本 = 全部输入版本的摘要。存在与期望版本一致的成功运行 →
  **复用**；否则 **待运行**，并逐条给出"输入 X 版本变化 a→b"的重跑原因；
- `impact_of(事件)` 按同一套读取声明做前向扩展，用于计划核对：晚到两天
  的站点修正只扩散到读取窗口覆盖它的日、周、县控与月分区；与变化无关且
  指纹未变的成功产物一律保留，绝不因收到一个文件就清空全县历史。

## 齐全、撤回与暂停

- 交付 `ready=false` 或修订 `complete=false` 使覆盖窗口进入等待；修订内容
  与齐全声明分开到达，`complete=true` 才恢复。等待中的分区**明确列出等待
  原因**（哪个交付未齐全、哪个声明被撤回），不会用旧资料或零需求跑出结果；
- `pause-source` 是调度闸：挡住需要新运行的分区（原因显示"已被操作员
  暂停"），已满足的产物与已发布的集合继续有效。

## 运行授权与恢复

- `begin_run` 先把 (run_token, 期望版本, 请求内容) 落库再写请求文件、启动
  演练程序；同一分区已有进行中授权时拒绝重复签发（RunConflict）；
- **进程退出、产物有效、可对外发布是三个不同事实**：退出码只记录；产物
  有效 = 回执存在、授权一致、回执记录的输入指纹与请求一致、产物字节摘要
  与回执一致；是否可发布由发布组闭包另行判断。`exit-after`（退出码 24）
  的回执有效即采纳；
- 运行期间新资料到达使期望版本变化时，在途授权的成功仍被记录，但**不能
  替新版本消除待办**，分区继续待运行；
- 重启后 `recover`：进行中的授权有有效回执则采纳（不漏掉已产出的结果），
  无回执则标记丢失并可重试（同一授权不会被执行第二次）。

## 成套发布

- 发布单元 = （发布组， 范围， 周期）：周期取组内最粗粒度（镇周/县周/镇月）；
- 发布闭包 = 期内成员分区 + 沿已满足输入链的全部作业分区。闭包内**全部
  当前满足**才发布，清单记录每个成员的运行授权与产物摘要——需求周实际
  读取的人员维度摘要与同套发布的人员维度产物一致，使用者不会读到
  "新人口配旧服务事实"；
- 部分失败 → 整套留在旧版本；其他乡镇没有共享变化时照常发布；县级控制
  缺口不阻挡独立乡镇；
- 每次发布写入 `outputs/published/<组>/<范围>/<周期>/v<N>/`（清单+产物
  副本），`LATEST` 指针原子切换；内容未变的组不重复出新版。

## 操作员命令

```
python -m orchestrator init            # 初始化状态库
python -m orchestrator ingest          # 装载 data/ 的交付与修订（幂等）
python -m orchestrator plan            # 可运行/等待分区与逐条原因
python -m orchestrator plan --impact D10 C1 C2   # 展开事件影响范围
python -m orchestrator run [--job J --scope-key T --partition P]
python -m orchestrator run --mode exit-after     # 演练异常退出
python -m orchestrator recover         # 重启恢复
python -m orchestrator publish         # 成套发布
python -m orchestrator status          # 状态计数与等待原因
python -m orchestrator explain person_dim T1 2026-10-15   # 单分区全量细节
python -m orchestrator retry   [--job J --scope-key T --partition P]
python -m orchestrator cancel  [--job J ...]     # 撤销未发布重算（产物保留）
python -m orchestrator pause-source population / resume-source population
python -m orchestrator published       # 已发布集合
```

默认状态目录 `state/`、产物目录 `outputs/`（均已 gitignore），可用
`--state-dir/--outputs-dir/--data-dir` 覆盖。

## 测试方式

```
python -m unittest discover -s tests -v
python examples/demo.py     # 端到端故事：基线→未齐全→修订→重算→v2
```

- `test_dependency_expansion.py`：两天人口/站点修正的影响集合与指纹复用；
- `test_readiness.py`：未齐全等待、齐全声明、撤回、来源暂停；
- `test_run_authorization.py`：真实子进程的 23/24 退出、重复授权拒绝、
  重启恢复、在途过期；
- `test_publication.py`：成套一致性、部分失败不半套出门、版本切换；
- `test_commitment_month.py`：跨月承诺与共享需求周；
- `test_cancel.py`：撤销重算与产物保留；`test_windows.py`：窗口算术；
- `test_materials.py`：来源资料自检（既有）。

多数测试注入协议兼容的进程内启动器（tests/helpers.py 的
`inline_launcher`）以省去子进程开销，回执校验路径与外部程序完全一致；
运行授权测试使用真实子进程验证外部协议本身。
