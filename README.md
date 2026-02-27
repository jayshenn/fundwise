# fundwise

基于 [AkShare](https://github.com/akfamily/akshare) 的投研框架文档仓库，面向 **A 股与港股**，按彼得林奇（Peter Lynch）的思路支持：

- 公司分析
- 选股
- 择时（节奏管理）

> 定位：投研决策支持，不是自动交易系统。

## 当前状态

本仓库当前以文档与最小 live 测试为主：

- 文档：`docs/`
- 最小真实网络测试：`tests/test_akshare_live_minimal.py`

## 技术选型与依赖管理

### 技术选型

- Python 3.11+
- 包管理与运行：`uv`
- 数据处理：`pandas`
- 数据源：`akshare`
- 测试框架：`pytest`

### 依赖管理（uv）

当前仓库已固定依赖：

```bash
# 初始化/更新虚拟环境
uv sync

# 运行最小 live 测试
uv run pytest -q tests/test_akshare_live_minimal.py -s
```

当你只想临时试跑某条命令（不修改项目依赖）时，也可以使用 `--with`：

```bash
# 临时注入依赖的运行方式（备用）
uv run --with akshare --with pandas --with matplotlib \
  python scripts/generate_structured_markdown_report.py --symbol 600519.SH
```

建议：

- 日常开发/测试：优先 `uv sync` + `uv run ...`
- 临时实验：按需使用 `uv run --with ...`

## 彼得林奇框架（落地版）

### 1) 公司分析（先判断是不是好公司）

核心维度：

- 成长性：营收/利润增长及稳定性
- 盈利质量：利润与现金流匹配度
- 财务健康：负债结构与偿债压力
- 估值合理性：PE/PB 相对历史和行业位置
- 风险项：商誉、事件冲击、行业景气变化

### 2) 选股（分类 + 打分）

建议流程：

- 按公司类型分层（慢增长/稳健增长/快速增长/周期/困境反转/资产型）
- 多因子打分（增长、质量、估值、风险）
- 输出“为什么值得关注”的证据链

### 3) 择时（轻择时，重估值与仓位）

建议原则：

- 不追求短期点位预测
- 用估值区间 + 分批建仓/减仓管理节奏
- 用行业资金流、行业指数、市场热度做辅助，不替代基本面

## 数据方法：先字典，再正文，再实测

按 [AKShare数据字典使用说明](docs/AKShare数据字典使用说明.md) 执行：

1. 在 `docs/AKShare数据字典.csv` / `.json` 召回候选接口
2. 用 `full_url` 定位 AKShare 文档正文
3. 映射到具体函数后，做真实网络调用测试

## 已实测通过的核心接口（2026-02-27）

以下接口已通过最小 live 测试（非空、关键列、新鲜度）：

- 行情：`stock_zh_a_hist_tx`
- 个股估值：`stock_zh_valuation_baidu`、`stock_hk_valuation_baidu`
- A 股三大报表：
  - `stock_balance_sheet_by_yearly_em`
  - `stock_profit_sheet_by_yearly_em`
  - `stock_cash_flow_sheet_by_yearly_em`
- 港股财务指标：`stock_financial_hk_analysis_indicator_em`
- 行业：
  - `stock_industry_pe_ratio_cninfo`
  - `stock_board_industry_name_ths`
  - `stock_board_industry_index_ths`
  - `stock_fund_flow_industry`

说明：上述接口与数据字典条目通过 `title + full_url` 做了可追溯校验。

## 辅助定性接口（抽测结果，2026-02-27）

以下接口可用于“定性判断的量化代理”，已做真实网络抽测：

- 舆情与披露：
  - `stock_news_em`（个股新闻）
  - `stock_notice_report`（公告）
  - `stock_report_disclosure`（财报披露排期）
- 研究观点：
  - `stock_research_report_em`（个股研报）
  - `stock_analyst_rank_em`（分析师排行）
  - `stock_analyst_detail_em`（分析师跟踪成分股）
- 管理层行为与资本配置：
  - `stock_yjyg_em`（业绩预告）
  - `stock_yjkb_em`（业绩快报）
  - `stock_repurchase_em`（股票回购）
  - `stock_hold_management_person_em`（董监高个人增减持）
- 公司治理风险：
  - `stock_cg_guarantee_cninfo`（对外担保）
  - `stock_cg_equity_mortgage_cninfo`（股权质押）
- 商誉风险专题：
  - `stock_sy_profile_em`（A股商誉市场概况）
  - `stock_sy_yq_em`（商誉减值预期明细，`date=20240630` 可用）
  - `stock_sy_jz_em`（个股商誉减值明细，`date=20231231` 可用）
  - `stock_sy_em`（个股商誉明细，`date=20231231` 可用）
  - `stock_sy_hy_em`（行业商誉，`date=20231231` 可用）

稳定性说明（建议在工程中做回退与容错）：

- `stock_cg_lawsuit_cninfo`：抽测出现 `KeyError: 'records'`。
- `stock_institute_recommend` / `stock_institute_recommend_detail`：源站页面结构变动，解析失败。
- `stock_institute_hold`：近期季度（如 `20251`）可能为空，历史季度（如 `20241`）可用。
- 商誉相关接口对报告期敏感，建议按 `20240930 -> 20240630 -> 20240331 -> 20231231` 回退。

定性打分落地建议：

- 将上述接口作为“定性代理信号”，不要直接替代人工判断。
- 评分形式建议为 `证据 -> 分项分数 -> 置信度(A/B/C)`。
- 设置红线项（重大违规、治理异常、持续减值恶化）一票否决。

## 林奇指标对照表（最小可行）

下表只使用本轮已 live 验证通过的接口，作为第一版可落地指标体系。

| 林奇维度 | 指标 | AkShare 接口（已实测） | 计算方式（建议） | 用途 |
| --- | --- | --- | --- | --- |
| 成长性 | 营收同比（YoY） | `stock_profit_sheet_by_yearly_em`、`stock_financial_hk_analysis_indicator_em` | `revenue_yoy = (本期营收/上年同期营收) - 1` | 判断公司是否持续增长 |
| 成长性 | 净利润同比（YoY） | `stock_profit_sheet_by_yearly_em`、`stock_financial_hk_analysis_indicator_em` | `profit_yoy = (本期净利润/上年同期净利润) - 1` | 判断增长质量是否同步 |
| 盈利能力 | ROE | `stock_financial_hk_analysis_indicator_em`（A 股可由利润表+资产负债表近似计算） | 港股优先直接取 `ROE_AVG`；A 股可用 `净利润/平均净资产` | 判断资本回报效率 |
| 盈利质量 | 经营现金流/净利润 | `stock_cash_flow_sheet_by_yearly_em` + `stock_profit_sheet_by_yearly_em` + `stock_financial_hk_analysis_indicator_em` | `cash_quality = OCF / NetProfit` | 识别“利润含金量” |
| 财务健康 | 资产负债率 | `stock_balance_sheet_by_yearly_em` | `debt_to_asset = 总负债 / 总资产` | 判断杠杆风险 |
| 估值 | PE(TTM) 当前值 | `stock_zh_valuation_baidu`、`stock_hk_valuation_baidu` | 直接读取最近交易日 `value` | 判断贵/便宜 |
| 估值 | PE 历史分位 | `stock_zh_valuation_baidu`、`stock_hk_valuation_baidu` | `pct = rank(当前PE, 历史PE序列)` | 与公司自身历史估值比较 |
| 估值 | 行业相对 PE | `stock_industry_pe_ratio_cninfo` + 个股估值接口 | `relative_pe = 个股PE / 行业PE` | 判断相对行业是否高估 |
| 选股打分 | 成长-估值匹配（PEG 近似） | 个股估值接口 + 利润表/财务指标接口 | `PEG ≈ PE / 利润增速(%)` | 识别“增长与估值错配” |
| 择时辅助 | 行业趋势 | `stock_board_industry_index_ths` | 可用 20/60 日均线或近 N 日收益率 | 辅助判断行业景气方向 |
| 择时辅助 | 行业资金净流入 | `stock_fund_flow_industry` | 直接使用行业 `净额` 排序 | 观察资金风险偏好 |
| 择时辅助 | 市场交易温度 | `stock_zh_a_hist_tx`（指数替代可扩展） | 统计涨跌幅分布、波动率、成交变化 | 控制仓位节奏 |

口径建议：

- 增长率与 PEG 建议同时看 3 年与 5 年窗口，减少单年波动干扰。
- A 股与港股字段命名不同，先做标准化映射再计算（不要直接在分析层依赖原始列名）。
- PEG 是近似值，不应单独决策，必须与现金流和负债指标联看。

## 最小 live 测试

运行命令：

```bash
uv run pytest -q tests/test_akshare_live_minimal.py -s
```

最近一次结果（2026-02-27）：

- `22 passed`

## 结构化 Markdown 报告输出（含图表）

新增脚本：

- `scripts/generate_structured_markdown_report.py`

示例命令（A 股）：

```bash
uv run python scripts/generate_structured_markdown_report.py \
  --symbol 600519.SH \
  --start-date 2025-01-01 \
  --end-date 2026-02-27 \
  --report-date 2026-02-27 \
  --out-dir reports/structured
```

示例命令（港股）：

```bash
uv run python scripts/generate_structured_markdown_report.py \
  --symbol 00700.HK \
  --start-date 2025-01-01 \
  --end-date 2026-02-27 \
  --report-date 2026-02-27 \
  --out-dir reports/structured
```

输出结构：

- `reports/structured/<report-date>/<symbol>/report-<symbol>-<report-date>.md`
- `reports/structured/<report-date>/<symbol>/charts/*.png`

报告包含：

- 执行摘要（价格、PE、营收、净利润）
- 核心数据快照
- 关键图表（价格趋势、PE 趋势、营收净利润趋势、风险指标趋势）
- 结论与跟踪建议

说明：

- 估值接口偶发网络/TLS 异常时，脚本会降级跳过 PE 图，不影响报告主流程。
- 若本机缺中文字体，图表可能出现中文字符告警，但图片仍会正常生成。

## 能力边界（与林奇框架对应）

AkShare 可较好覆盖“量化部分”：

- 行情/估值/财报/行业对比/资金流

AkShare 对“定性部分”可提供量化代理信号（新闻、公告、研报、增减持、回购、商誉、治理事件），但仍不能替代人工判断的核心问题：

- 管理层质量
- 产品竞争力与护城河
- 一线调研信息（渠道、用户体验等）

因此建议：

- 用 AkShare 做量化底座
- 用公告、纪要、新闻和人工研究补齐定性判断

## 文档索引

- [AKShare数据字典使用说明](docs/AKShare数据字典使用说明.md)
- [AKShare数据字典.csv](docs/AKShare数据字典.csv)
- [AKShare数据字典.json](docs/AKShare数据字典.json)
- [AkShare接口可用性与字段校验说明](docs/AkShare接口可用性与字段校验说明.md)
