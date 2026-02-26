# AkShare 接口可用性与字段校验说明

本文档定义本项目对 AkShare 的 live 校验范围，目标是确保核心接口可用、核心字段稳定、关键数据不过期。

## 校验目标

- 验证接口可访问且返回非空 DataFrame。
- 验证关键字段存在，避免上游字段变更导致分析模块失效。
- 验证日频行情与估值数据不过期（最近 45 天内有数据）。

## 覆盖接口（A 股 + 港股）

| 维度 | 接口 | 用途 | 关键字段 |
| --- | --- | --- | --- |
| A 股行情 | `stock_zh_a_hist_tx` | 价格与成交额时间序列（腾讯源） | `date`, `open`, `close`, `high`, `low`, `amount` |
| 港股行情 | `stock_hk_daily` | 价格与成交量时间序列 | `date`, `open`, `high`, `low`, `close`, `volume` |
| A 股估值 | `stock_zh_valuation_baidu` | 市值/PE/PB 等历史估值 | `date`, `value` |
| 港股估值 | `stock_hk_valuation_baidu` | 市值/PE/PB 等历史估值 | `date`, `value` |
| A 股财报 | `stock_balance_sheet_by_yearly_em` | 资产负债表 | `SECUCODE`, `SECURITY_CODE`, `REPORT_DATE` |
| A 股财报 | `stock_profit_sheet_by_yearly_em` | 利润表 | `SECUCODE`, `SECURITY_CODE`, `REPORT_DATE` |
| A 股财报 | `stock_cash_flow_sheet_by_yearly_em` | 现金流量表 | `SECUCODE`, `SECURITY_CODE`, `REPORT_DATE` |
| 港股财务指标 | `stock_financial_hk_analysis_indicator_em` | ROE、利润、现金流、负债率 | `SECUCODE`, `REPORT_DATE`, `OPERATE_INCOME`, `HOLDER_PROFIT`, `ROE_AVG`, `PER_NETCASH_OPERATE`, `DEBT_ASSET_RATIO` |
| 港股报表 | `stock_financial_hk_report_em` | 资产负债表明细 | `SECUCODE`, `SECURITY_CODE`, `STD_ITEM_NAME`, `AMOUNT`, `STD_REPORT_DATE` |

## 覆盖接口（行业数据）

| 维度 | 接口 | 用途 | 关键字段 |
| --- | --- | --- | --- |
| 行业列表 | `stock_board_industry_name_ths` | 行业名称与编码 | `name`, `code` |
| 行业指数 | `stock_board_industry_index_ths` | 行业板块历史行情 | `日期`, `开盘价`, `最高价`, `最低价`, `收盘价`, `成交量`, `成交额` |
| 行业资金流 | `stock_fund_flow_industry` | 行业资金流向（即时） | `行业`, `行业指数`, `行业-涨跌幅`, `流入资金`, `流出资金`, `净额`, `公司家数`, `领涨股` |
| 行业市盈率 | `stock_industry_pe_ratio_cninfo` | 行业估值（证监会行业分类） | `变动日期`, `行业分类`, `行业编码`, `行业名称`, `公司数量`, `静态市盈率-加权平均` |

## 测试代码位置

- `tests/test_akshare_live_contracts.py`
- `tests/test_akshare_live_industry_contracts.py`

## 运行方式（uv）

```bash
# 默认模式：不跑 live（会 skip）
uv run pytest -q

# 开启 live 校验（需要网络）
uv run env RUN_AKSHARE_TESTS=1 pytest -q -m akshare_live -s

# 严格模式：网络/上游异常直接失败（默认会 skip）
uv run env RUN_AKSHARE_TESTS=1 AKSHARE_STRICT=1 pytest -q -m akshare_live -s
```

## 结果解释

- 通过：接口可用，字段满足当前契约。
- 失败（字段缺失）：上游接口字段可能变化，需要更新 `data_adapter` 映射。
- 失败（空数据或过期）：可能是上游数据源异常、节假日延迟或接口变更。
- skip（仅非严格模式）：网络/TLS 或上游临时故障，建议重试或切换到严格模式复核。

## 行业数据说明

- 行业市盈率接口按阶段披露，测试会在最近多个季度末日期中回退查找可用数据。
- 行业数据测试代码位于 `tests/test_akshare_live_industry_contracts.py`。

## 与 symbol 规范的关系

项目内部 symbol 统一为 `600519.SH` / `00700.HK`。  
AkShare 某些接口需要不同参数格式（如 `600519`、`SH600519`、`00700`），在 `data_adapter` 层做转换，不在分析层直接处理。

## 文档来源（官方）

- https://akshare.akfamily.xyz/data/stock/stock.html
- https://github.com/akfamily/akshare
