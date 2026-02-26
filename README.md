# fundwise

借助 [AkShare](https://github.com/akfamily/akshare) 构建面向 **A 股与港股** 的投研辅助系统，支持公司分析、选股和择时决策。

## 项目定位

- 这是一个 **投研决策支持** 项目，不是自动下单的量化交易系统。
- 目标是把分散的数据转化为结构化分析结论，帮助提升研究效率与决策质量。

## 核心目标

- 公司分析：构建公司体检卡（成长性、盈利质量、现金流、估值区间、风险提示）。
- 选股支持：建立候选池与评分卡，输出“为什么值得关注”。
- 择时支持：给出市场状态判断与仓位建议区间（低/中/高仓）。
- 研究输出：自动生成日报/周报（观察池、重点公司、市场温度）。

## 非目标

- 不做自动交易执行。
- 不做高频或超短线策略系统。
- 不以“预测短期价格”为核心目标。

## 计划中的核心模块

- `company_dossier`：单公司深度分析与跟踪。
- `watchlist_screener`：多维评分筛选与候选池排序。
- `market_timing_panel`：市场状态与风险偏好面板。
- `report_engine`：结构化研究报告生成。

## 数据范围

- 市场：A 股、港股。
- 数据类型：行情、估值、财务、资金流、行业分类、关键事件（按可用性逐步扩展）。

## 当前阶段

当前已完成 `uv` 依赖管理、AkShare live 接口校验框架、`data_adapter` 标准化层、SQLite 元数据索引（含 `symbols` 股票池登记、`data_jobs` 任务日志与 `fx_rates` 汇率表）、单公司分析卡输出、观察池评分报告（`watchlist_screener`）MVP、市场择时面板（`market_timing_panel`）MVP，以及 `report_engine` 的 `matplotlib` 图表输出（PNG + Markdown 嵌入）。

## Python 依赖管理（uv）

本项目使用 [uv](https://docs.astral.sh/uv/) 管理 Python 版本与依赖。

```bash
# 首次初始化（会根据 pyproject.toml 创建 .venv）
uv sync

# 安装运行时依赖（示例）
uv add pandas matplotlib

# 安装开发依赖（加入 dev 组）
uv add --group dev jupyterlab

# 在项目环境中运行命令
uv run python -c "import akshare as ak; print('akshare ok')"

# 初始化本地 SQLite 元数据库
uv run python scripts/init_sqlite.py

# 拉取单公司标准化数据并写入快照索引
uv run python scripts/fetch_company_dataset.py --symbol 600519.SH --start-date 2024-01-01

# 生成单公司分析卡（Markdown + 图表）并登记报告索引
uv run python scripts/generate_company_report.py --symbol 600519.SH --start-date 2024-01-01

# 生成观察池评分报告（Markdown + CSV + 图表）并登记报告索引
uv run python scripts/generate_watchlist_report.py --symbols 600519.SH,000333.SZ,00700.HK --start-date 2024-01-01

# 生成市场择时面板报告（Markdown + CSV + 图表）并登记报告索引
uv run python scripts/generate_market_timing_report.py --symbols 600519.SH,000333.SZ,00700.HK --start-date 2024-01-01

# 写入汇率（示例：2026-02-26 的 HKD->CNY）
uv run python scripts/upsert_fx_rate.py --date 2026-02-26 --base-currency HKD --quote-currency CNY --rate 0.92 --source manual

# 执行日常批处理流水线（按日期归档全部产物）
uv run python scripts/run_daily_pipeline.py --symbols 600519.SH,000333.SZ,00700.HK --start-date 2024-01-01 --run-date 2026-02-26

# 检查流水线健康状态（可用于 cron/CI 告警）
uv run python scripts/check_pipeline_health.py --max-delay-hours 36

# 生成流水线运行历史报告（运维视图）
uv run python scripts/generate_pipeline_history_report.py --limit 30
```

依赖声明位于 `pyproject.toml`，建议将 `uv.lock` 提交到版本库以确保团队环境一致。

## 开发规范与工具链

当前技术栈：

- 依赖与环境管理：`uv`
- 项目配置：`pyproject.toml`
- 代码检查与格式化：`ruff`
- 测试框架：`pytest`
- 类型检查（进阶）：`pyright`

文档与注释约定：

- 后续新增代码的 `docstring`、代码注释、命令行提示与报告文本默认使用中文。
- 历史遗留英文说明逐步迁移为中文；必要时可保留专有名词（如 `AkShare`、`SQLite`）。

常用命令：

```bash
# 代码检查
uv run ruff check .

# 自动格式化
uv run ruff format .

# 单元/集成测试
uv run pytest -q

# 类型检查
uv run pyright
```

### AkShare 接口校验测试

```bash
# 默认：不跑 live（会 skip）
uv run pytest -q

# live 校验（网络异常默认 skip）
uv run env RUN_AKSHARE_TESTS=1 pytest -q -m akshare_live -s

# 仅跑行业接口校验
uv run env RUN_AKSHARE_TESTS=1 pytest -q tests/test_akshare_live_industry_contracts.py -s

# 严格模式（网络/上游异常直接 fail）
uv run env RUN_AKSHARE_TESTS=1 AKSHARE_STRICT=1 pytest -q -m akshare_live -s
```

上述命令会覆盖个股与行业接口的可用性、字段契约和基础数据新鲜度校验。  
详细说明见：`docs/AkShare接口可用性与字段校验说明.md`

## 文档索引

- `docs/架构与设计说明.md`
- `docs/AkShare接口可用性与字段校验说明.md`
- `docs/AIDC&IDC 算力观察池（A股+港股）.md`

## 单公司 8 图清单（A 股 + 港股）

以下图表用于单公司长期趋势研判，默认输出 PNG 并嵌入 Markdown 报告：

| 图表 | 核心口径 | 主要数据来源（AkShare） |
| --- | --- | --- |
| 市值 vs 营收趋势 | 总市值（线）+ 总营收（柱） | 个股估值 + 利润表 |
| 主营收 vs 经营现金流趋势 | 主营收、经营活动现金流净额 | 利润表 + 现金流量表 |
| 净利润 vs 经营现金流趋势 | 归母净利润、经营活动现金流净额 | 利润表 + 现金流量表 |
| 市盈率对比趋势 | 公司 PE、行业 PE、沪深300 PE | 个股估值 + 行业 PE + 指数 PE |
| PE 均值与标准差区间 | 公司 PE、8 年均值、±1σ | 个股估值 |
| ROE 对比趋势 | 公司 ROE、行业 ROE（中位数） | 财务分析指标 + 行业成分股 |
| 资产负债结构（单期） | 资产/负债科目合并同类项 | 资产负债表 |
| 总资产/总负债/资产负债率趋势 | 总资产、总负债、资产负债率 | 资产负债表（资产负债率可计算） |

## 字段映射建议（统一内部字段）

为减少 A 股与港股接口差异，建议先做一层标准化字段：

| 内部字段 | 常见原始字段（示例） | 备注 |
| --- | --- | --- |
| `symbol` | 代码 | 建议统一为 `600519.SH` / `00700.HK` 这类通用格式 |
| `date` | 报告期/交易日 | 统一为 `YYYY-MM-DD` |
| `market_cap` | 总市值 | 尽量使用同一币种；跨市场时保留 `currency` |
| `revenue` | 营业总收入/主营业务收入 | 需固定一种口径 |
| `net_profit` | 归母净利润/净利润 | 优先归母口径 |
| `ocf` | 经营活动现金流净额 | 对应现金流量表 |
| `pe_ttm` | 市盈率(TTM) | 若无 TTM，可降级使用静态 PE |
| `industry_pe` | 行业市盈率 | 低频快照，绘图时前向填充 |
| `hs300_pe` | 沪深300市盈率 | 指数估值基准线 |
| `roe` | ROE(%) | 保持加权平均或摊薄口径一致 |
| `industry_roe` | 行业 ROE（中位数） | 基于行业成分股聚合计算 |
| `total_assets` | 资产总计 | 来自资产负债表 |
| `total_liabilities` | 负债合计 | 来自资产负债表 |
| `debt_to_asset` | 资产负债率 | 建议统一按 `total_liabilities / total_assets` 计算 |
| `currency` | 币种 | A 股通常 `CNY`，港股通常 `HKD` |

> 注：AkShare 不同接口字段命名与代码参数格式可能略有差异，建议在 `data_adapter` 层做列名映射、代码格式转换与口径校验，不在分析层直接依赖原始列名。

## 跨币种折算说明

- 若在 `fx_rates` 中写入了币种到 `CNY` 的汇率，单公司报告会自动输出 CNY 折算字段（市值/营收/净利润/经营现金流）。
- `CNY` 标的默认按 `1.0` 折算，无需额外写汇率。
- 非 `CNY` 标的若缺失汇率，报告会保留原币种并将 CNY 折算值显示为 `N/A`。

## 行业数据主备策略

- 主数据源：AkShare 实时行业接口（行业 PE、行业成分股、行业 ROE 聚合）。
- 备数据源：本地缓存快照（`data/cache/industry/*.csv`）。
- 回退规则：当实时接口返回空数据或临时异常时，自动回退读取最近缓存，不中断报告流程。
- 缓存写入：当实时拉取成功后，自动覆盖写入对应缓存文件，供后续主源异常时兜底。

## 日常流水线说明

- `scripts/run_daily_pipeline.py` 会串联执行：单标的拉数与快照索引、单公司报告、观察池报告、择时面板、执行摘要归档。
- 默认归档目录：
  - 报告：`reports/daily/<run-date>/...`
  - 标准化数据：`data/normalized/<run-date>/...`
- 默认策略为“有失败标的即返回非 0 退出码”，便于对接 `cron`/CI 告警。
- 若希望部分失败仍返回 0，可附加 `--allow-partial-success`。
- 可通过 `scripts/check_pipeline_health.py` 做独立健康巡检（失败/超时/过期返回非 0）。
- 可通过 `scripts/generate_pipeline_history_report.py` 生成最近任务历史报告并登记到报告索引。
