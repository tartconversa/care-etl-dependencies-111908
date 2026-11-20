# 照护供需仓的按范围重算编排资料包

离线作业编排项目：把原始输入装载、人员与站点维表、服务事实及乡镇供需
数据集串成可持续运转的数仓。编排层决定哪些范围具备运行条件、一次变化
要求重做哪些下游，以及什么时候一套新资料可以让使用者读到。

## 内容

- `orchestrator/` 编排实现：来源事件簿、分区派生与依赖传播、运行授权、
  成套发布；设计约定见 docs/orchestration.md。
- `data/` 合成来源资料，`docs/source-contract.md` 解释来源身份、适用范围
  和业务含义；`support/check_inputs.py` 只做资料完整性自检。
- `support/job_worker.py` 外部演练程序，按作业协议写出产物回执。
- `tests/` 资料自检、依赖传播、运行授权、成套发布等测试。
- `examples/demo.py` 端到端故事演练。

## 环境与使用

Python 3.14 标准库，无需在线安装依赖。Dockerfile 锁定基础镜像完整摘要；
首次构建需要本地镜像或拉取该摘要。

    python support/check_inputs.py          # 资料自检
    python -m unittest discover -s tests -v # 全部测试
    python examples/demo.py                 # 端到端演练（写 state/ 与 outputs/）

日常编排入口：

    python -m orchestrator init && python -m orchestrator ingest
    python -m orchestrator plan            # 哪些范围可运行、哪些在等待、为何
    python -m orchestrator run           # 驱动外部演练程序
    python -m orchestrator publish       # 成套发布
    python -m orchestrator status        # 每个范围在等什么
    python -m orchestrator explain person_dim T1 2026-10-15

状态目录 `state/`、产物目录 `outputs/` 为本地演练目录（已 gitignore），
可用 `--state-dir/--outputs-dir/--data-dir` 覆盖。操作员命令（重试、暂停
来源、撤销未发布重算、重启恢复）见 docs/orchestration.md。

    docker build -t care-etl-dependencies-inputs .
    docker run --rm --network none care-etl-dependencies-inputs

容器默认进行资料自检；可在镜像名后加 `python -m unittest discover -s tests -v`
运行全部测试，或加 `python examples/demo.py` 跑端到端演练。
