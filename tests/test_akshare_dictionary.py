"""AKShare 数据字典解析测试。"""

from __future__ import annotations

import json
from pathlib import Path

from fundwise.data_adapter.akshare_dictionary import (
    export_akshare_dictionary,
    parse_akshare_dictionary,
)

SAMPLE_HTML = """
<html>
  <body>
    <div itemprop="articleBody">
      <div class="toctree-wrapper compound">
        <ul>
          <li class="toctree-l1">
            <a class="reference internal" href="stock/stock.html">股票</a>
            <ul>
              <li class="toctree-l2">
                <a class="reference internal" href="stock/stock.html#a">A股</a>
              </li>
              <li class="toctree-l2">
                <a class="reference internal" href="stock/stock.html#id2"></a>
              </li>
            </ul>
          </li>
          <li class="toctree-l1">
            <a class="reference internal" href="bond/bond.html">债券</a>
          </li>
        </ul>
      </div>
    </div>
  </body>
</html>
"""


def test_parse_akshare_dictionary_hierarchy() -> None:
    nodes = parse_akshare_dictionary(SAMPLE_HTML)

    assert len(nodes) == 4
    assert nodes[0].title == "股票"
    assert nodes[0].path == "股票"
    assert not nodes[0].is_leaf

    assert nodes[1].parent_title == "股票"
    assert nodes[1].root_title == "股票"
    assert nodes[1].path == "股票 / A股"
    assert nodes[1].full_url == "https://akshare.akfamily.xyz/data/stock/stock.html#a"
    assert nodes[1].is_leaf

    assert nodes[2].title == "(空标题)"
    assert nodes[2].is_leaf

    assert nodes[3].title == "债券"
    assert nodes[3].is_leaf


def test_export_akshare_dictionary_outputs(tmp_path: Path) -> None:
    html_path = tmp_path / "dict.html"
    csv_path = tmp_path / "dict.csv"
    json_path = tmp_path / "dict.json"
    html_path.write_text(SAMPLE_HTML, encoding="utf-8")

    node_count, leaf_count = export_akshare_dictionary(
        html_path=html_path,
        csv_path=csv_path,
        json_path=json_path,
    )

    assert node_count == 4
    assert leaf_count == 3
    assert csv_path.exists()
    assert json_path.exists()

    csv_text = csv_path.read_text(encoding="utf-8")
    assert "full_url" in csv_text
    assert "parent_title" in csv_text
    assert "is_leaf" in csv_text

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["source_path"] == str(html_path)
    assert payload["node_count"] == 4
    assert payload["leaf_count"] == 3
    assert payload["nodes"][0]["title"] == "股票"
    assert payload["nodes"][0]["full_url"] == "https://akshare.akfamily.xyz/data/stock/stock.html"
