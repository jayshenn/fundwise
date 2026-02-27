# AKShare 数据字典使用说明

本文说明以下文件如何配合使用，服务于 AI 大模型按需检索 AKShare 文档并获取内容：

- `docs/AKShare数据字典.html`
- `docs/AKShare数据字典.csv`
- `docs/AKShare数据字典.json`

## 1. 文件定位

- `AKShare数据字典.html`：原始目录页（Sphinx toctree）。
- `AKShare数据字典.csv`：扁平化节点表，适合关键词过滤与排序。
- `AKShare数据字典.json`：结构化节点集合，适合程序消费与上下文打包。

## 2. 如何更新

当 `docs/AKShare数据字典.html` 更新后，执行：

```bash
uv run python scripts/export_akshare_dictionary.py
```

输出会覆盖：

- `docs/AKShare数据字典.csv`
- `docs/AKShare数据字典.json`

## 3. 字段说明（CSV/JSON 通用）

- `index`：节点顺序号（从 1 开始）。
- `level`：目录层级（`toctree-l1/l2/...`）。
- `title`：节点标题；空标题会被写为 `(空标题)`。
- `href`：相对链接（如 `stock/stock.html#id24`）。
- `full_url`：完整链接（如 `https://akshare.akfamily.xyz/data/stock/stock.html#id24`）。
- `doc_path`：文档路径（不含锚点，如 `stock/stock.html`）。
- `anchor`：锚点（如 `id24`；无锚点则为空）。
- `parent_title`：父节点标题。
- `root_title`：一级分类标题（如 `AKShare 股票数据`）。
- `path`：完整层级路径（如 `AKShare 股票数据 / A股 / 历史行情数据`）。
- `is_leaf`：是否叶子节点；`1/true` 通常表示最具体的接口条目。

## 4. AI 推荐检索流程

建议分两阶段：

1. 候选召回（基于 `csv/json`）
2. 正文读取（优先使用 `full_url`）

推荐策略：

- 优先过滤 `is_leaf=1`，减少泛目录噪声。
- 同时匹配 `title` 和 `path`，提高召回准确度。
- 先按关键词得分排序，再按 `doc_path` 去重，避免同页重复读取。
- 最终保留 Top-N（建议 3-8）再读正文，控制上下文长度。

## 5. Python 最小示例（召回）

```python
from pathlib import Path
import pandas as pd

df = pd.read_csv(Path("docs/AKShare数据字典.csv"))
keywords = ["A股", "历史行情", "东财"]

mask = df["is_leaf"].astype(str).isin(["1", "True", "true"])
for kw in keywords:
    mask &= df["path"].str.contains(kw, na=False) | df["title"].str.contains(kw, na=False)

candidates = (
    df.loc[mask, ["title", "path", "full_url", "doc_path", "anchor"]]
    .drop_duplicates(subset=["doc_path", "anchor"])
    .head(8)
)
print(candidates.to_markdown(index=False))
```

## 6. 正文定位建议

推荐优先使用 `full_url` 直接抓取正文；若只使用 `href`，可按以下规则补全：

- `href = stock/stock.html#id24`
- 完整地址：`https://akshare.akfamily.xyz/data/stock/stock.html#id24`
- 锚点：`id24`

如果本地已镜像 AKShare 文档，可直接读取本地 HTML；在线读取时可直接使用 `full_url`。

## 7. 在本仓库中的推荐用法

- 数据适配开发前：先在 `csv/json` 里定位候选接口，减少盲查。
- 新增/修复接口时：将使用到的 `href` 记录到开发说明或测试注释，便于后续回溯。
- AI 辅助编码时：将“候选接口列表 + 对应正文片段”一起提供给模型，通常比仅给接口名更稳定。
