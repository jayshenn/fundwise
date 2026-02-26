"""初始化本地 SQLite 元数据库。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def parse_args() -> argparse.Namespace:
    """解析 SQLite 初始化命令行参数。"""
    parser = argparse.ArgumentParser(description="初始化 fundwise 的 SQLite 元数据库。")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="自定义 SQLite 数据库文件路径。",
    )
    return parser.parse_args()


def main() -> int:
    """使用指定路径或默认路径初始化元数据库。

    返回：
        进程退出码；成功返回 0。
    """
    from fundwise.storage.sqlite_store import get_default_db_path, init_sqlite_metadata_db

    args = parse_args()
    db_path = args.db_path if args.db_path is not None else get_default_db_path(PROJECT_ROOT)
    target = init_sqlite_metadata_db(db_path)
    print(f"SQLite 元数据库初始化完成：{target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
