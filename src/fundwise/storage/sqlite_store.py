"""SQLite 元数据库初始化与写入辅助函数。

该模块负责 MVP 阶段的元数据存储：
1. 初始化元数据库表结构；
2. 写入数据快照索引；
3. 写入报告索引。

明细时序数据仍存放在文件（如 parquet/csv）中，SQLite 仅存储索引与元信息。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Final

METADATA_TABLES: Final[tuple[str, ...]] = (
    "symbols",
    "data_jobs",
    "dataset_snapshots",
    "reports",
    "fx_rates",
)

JOB_RUNNING: Final[str] = "running"
JOB_SUCCESS: Final[str] = "success"
JOB_FAILED: Final[str] = "failed"
JOB_STATUSES: Final[tuple[str, ...]] = (JOB_RUNNING, JOB_SUCCESS, JOB_FAILED)

_DDL_STATEMENTS: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS symbols (
        symbol TEXT PRIMARY KEY,
        market TEXT NOT NULL,
        name TEXT,
        currency TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS data_jobs (
        job_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_type TEXT NOT NULL,
        symbol TEXT,
        status TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        error_message TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(symbol) REFERENCES symbols(symbol)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dataset_snapshots (
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        dataset_type TEXT NOT NULL,
        as_of_date TEXT NOT NULL,
        file_path TEXT NOT NULL,
        row_count INTEGER NOT NULL DEFAULT 0,
        checksum TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(symbol) REFERENCES symbols(symbol),
        UNIQUE(symbol, dataset_type, as_of_date, file_path)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS reports (
        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        report_type TEXT NOT NULL,
        report_date TEXT NOT NULL,
        file_path TEXT NOT NULL,
        generated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(symbol) REFERENCES symbols(symbol),
        UNIQUE(symbol, report_type, report_date, file_path)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS fx_rates (
        date TEXT NOT NULL,
        base_currency TEXT NOT NULL,
        quote_currency TEXT NOT NULL,
        rate REAL NOT NULL,
        source TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (date, base_currency, quote_currency)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_data_jobs_symbol_status ON data_jobs(symbol, status);",
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_type_date ON "
        "dataset_snapshots(symbol, dataset_type, as_of_date);"
    ),
    "CREATE INDEX IF NOT EXISTS idx_reports_symbol_date ON reports(symbol, report_date);",
)


def _connect(db_path: str | Path) -> sqlite3.Connection:
    """创建 SQLite 连接并开启外键约束。"""
    conn = sqlite3.connect(Path(db_path).expanduser().resolve())
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_default_db_path(project_root: Path | None = None) -> Path:
    """返回默认的 SQLite 元数据库路径。

    参数说明：
        project_root：可选的项目根目录；为空时使用 `Path.cwd()`。

    返回：
        元数据库文件的绝对路径。
    """
    root = project_root if project_root is not None else Path.cwd()
    return root / "data" / "metadata" / "fundwise.db"


def init_sqlite_metadata_db(db_path: str | Path | None = None) -> Path:
    """初始化 SQLite 元数据库及必需表结构。

    参数说明：
        db_path：可选数据库路径；为空时使用当前工作目录下
            `data/metadata/fundwise.db`。

    返回：
        已初始化数据库文件的绝对路径。
    """
    target = Path(db_path) if db_path is not None else get_default_db_path()
    target = target.expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)

    conn = _connect(target)
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        for statement in _DDL_STATEMENTS:
            conn.execute(statement)
        conn.commit()
    finally:
        conn.close()

    return target


def record_dataset_snapshot(
    db_path: str | Path,
    *,
    symbol: str,
    dataset_type: str,
    as_of_date: str,
    file_path: str | Path,
    row_count: int,
    checksum: str | None = None,
) -> None:
    """写入或更新数据快照索引记录。"""
    target_db = Path(db_path).expanduser().resolve()
    normalized_file_path = str(Path(file_path).expanduser().resolve())
    conn = _connect(target_db)
    try:
        conn.execute(
            """
            INSERT INTO dataset_snapshots (
                symbol,
                dataset_type,
                as_of_date,
                file_path,
                row_count,
                checksum
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, dataset_type, as_of_date, file_path)
            DO UPDATE SET
                row_count = excluded.row_count,
                checksum = excluded.checksum,
                created_at = CURRENT_TIMESTAMP;
            """,
            (symbol, dataset_type, as_of_date, normalized_file_path, row_count, checksum),
        )
        conn.commit()
    finally:
        conn.close()


def record_report(
    db_path: str | Path,
    *,
    symbol: str,
    report_type: str,
    report_date: str,
    file_path: str | Path,
) -> None:
    """写入或更新报告索引记录。"""
    target_db = Path(db_path).expanduser().resolve()
    normalized_file_path = str(Path(file_path).expanduser().resolve())
    conn = _connect(target_db)
    try:
        conn.execute(
            """
            INSERT INTO reports (
                symbol,
                report_type,
                report_date,
                file_path
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol, report_type, report_date, file_path)
            DO UPDATE SET
                generated_at = CURRENT_TIMESTAMP;
            """,
            (symbol, report_type, report_date, normalized_file_path),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_symbol(
    db_path: str | Path,
    *,
    symbol: str,
    market: str,
    currency: str,
    name: str | None = None,
    is_active: bool = True,
) -> None:
    """写入或更新股票池符号基础信息。"""
    target_db = Path(db_path).expanduser().resolve()
    conn = _connect(target_db)
    try:
        conn.execute(
            """
            INSERT INTO symbols (symbol, market, name, currency, is_active)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol)
            DO UPDATE SET
                market = excluded.market,
                name = excluded.name,
                currency = excluded.currency,
                is_active = excluded.is_active,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (symbol, market, name, currency, 1 if is_active else 0),
        )
        conn.commit()
    finally:
        conn.close()


def list_symbols(
    db_path: str | Path,
    *,
    only_active: bool = True,
    market: str | None = None,
) -> list[dict[str, str | int | None]]:
    """查询股票池符号列表。"""
    target_db = Path(db_path).expanduser().resolve()
    conn = _connect(target_db)
    try:
        query = (
            "SELECT symbol, market, name, currency, is_active, updated_at "
            "FROM symbols WHERE 1=1"
        )
        params: list[str | int] = []
        if only_active:
            query += " AND is_active = 1"
        if market is not None:
            query += " AND market = ?"
            params.append(market)
        query += " ORDER BY symbol ASC"

        rows = conn.execute(query, params).fetchall()
        result: list[dict[str, str | int | None]] = []
        for row in rows:
            result.append(
                {
                    "symbol": str(row[0]),
                    "market": str(row[1]),
                    "name": None if row[2] is None else str(row[2]),
                    "currency": str(row[3]),
                    "is_active": int(row[4]),
                    "updated_at": str(row[5]),
                }
            )
        return result
    finally:
        conn.close()


def start_data_job(
    db_path: str | Path,
    *,
    job_type: str,
    symbol: str | None = None,
) -> int:
    """创建数据任务日志并返回 job_id。"""
    target_db = Path(db_path).expanduser().resolve()
    conn = _connect(target_db)
    try:
        cursor = conn.execute(
            """
            INSERT INTO data_jobs (
                job_type,
                symbol,
                status,
                started_at
            )
            VALUES (?, ?, ?, CURRENT_TIMESTAMP);
            """,
            (job_type, symbol, JOB_RUNNING),
        )
        conn.commit()
        if cursor.lastrowid is None:
            raise RuntimeError("创建数据任务失败：未返回 job_id")
        return int(cursor.lastrowid)
    finally:
        conn.close()


def finish_data_job(
    db_path: str | Path,
    *,
    job_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """更新数据任务状态。"""
    if status not in JOB_STATUSES:
        raise ValueError(f"不支持的任务状态: {status}")

    target_db = Path(db_path).expanduser().resolve()
    conn = _connect(target_db)
    try:
        conn.execute(
            """
            UPDATE data_jobs
            SET
                status = ?,
                finished_at = CURRENT_TIMESTAMP,
                error_message = ?
            WHERE job_id = ?;
            """,
            (status, error_message, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def list_data_jobs(
    db_path: str | Path,
    *,
    job_type: str | None = None,
    status: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
) -> list[dict[str, str | int | None]]:
    """查询数据任务日志。"""
    if limit <= 0:
        raise ValueError("limit 必须大于 0")

    target_db = Path(db_path).expanduser().resolve()
    conn = _connect(target_db)
    try:
        query = (
            "SELECT job_id, job_type, symbol, status, started_at, finished_at, "
            "error_message, created_at "
            "FROM data_jobs WHERE 1=1"
        )
        params: list[str | int] = []
        if job_type is not None:
            query += " AND job_type = ?"
            params.append(job_type)
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        if symbol is not None:
            query += " AND symbol = ?"
            params.append(symbol)
        query += " ORDER BY job_id DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        result: list[dict[str, str | int | None]] = []
        for row in rows:
            result.append(
                {
                    "job_id": int(row[0]),
                    "job_type": str(row[1]),
                    "symbol": None if row[2] is None else str(row[2]),
                    "status": str(row[3]),
                    "started_at": None if row[4] is None else str(row[4]),
                    "finished_at": None if row[5] is None else str(row[5]),
                    "error_message": None if row[6] is None else str(row[6]),
                    "created_at": None if row[7] is None else str(row[7]),
                }
            )
        return result
    finally:
        conn.close()


def upsert_fx_rate(
    db_path: str | Path,
    *,
    date: str,
    base_currency: str,
    quote_currency: str,
    rate: float,
    source: str,
) -> None:
    """写入或更新汇率记录。"""
    if rate <= 0:
        raise ValueError("汇率 rate 必须大于 0")

    normalized_base = base_currency.strip().upper()
    normalized_quote = quote_currency.strip().upper()
    normalized_source = source.strip()
    if not normalized_base or not normalized_quote:
        raise ValueError("base_currency 与 quote_currency 不能为空")
    if not normalized_source:
        raise ValueError("source 不能为空")

    target_db = Path(db_path).expanduser().resolve()
    conn = _connect(target_db)
    try:
        conn.execute(
            """
            INSERT INTO fx_rates (
                date,
                base_currency,
                quote_currency,
                rate,
                source
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date, base_currency, quote_currency)
            DO UPDATE SET
                rate = excluded.rate,
                source = excluded.source,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (
                date,
                normalized_base,
                normalized_quote,
                rate,
                normalized_source,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_fx_rate(
    db_path: str | Path,
    *,
    date: str | None,
    base_currency: str,
    quote_currency: str,
    allow_nearest: bool = True,
) -> float | None:
    """查询汇率，支持反向汇率自动倒数与最近日期回退。"""
    normalized_base = base_currency.strip().upper()
    normalized_quote = quote_currency.strip().upper()
    if not normalized_base or not normalized_quote:
        return None
    if normalized_base == normalized_quote:
        return 1.0

    target_db = Path(db_path).expanduser().resolve()
    conn = _connect(target_db)
    try:
        direct_rate = _query_fx_rate(
            conn=conn,
            date=date,
            base_currency=normalized_base,
            quote_currency=normalized_quote,
            allow_nearest=allow_nearest,
        )
        if direct_rate is not None:
            return direct_rate

        reverse_rate = _query_fx_rate(
            conn=conn,
            date=date,
            base_currency=normalized_quote,
            quote_currency=normalized_base,
            allow_nearest=allow_nearest,
        )
        if reverse_rate is None or reverse_rate == 0:
            return None
        return 1.0 / reverse_rate
    finally:
        conn.close()


def resolve_fx_to_cny(
    db_path: str | Path,
    *,
    currency: str,
    date: str | None,
    allow_nearest: bool = True,
) -> float | None:
    """解析目标币种对 CNY 的换算汇率。"""
    normalized_currency = currency.strip().upper()
    if not normalized_currency:
        return None
    if normalized_currency == "CNY":
        return 1.0
    return get_fx_rate(
        db_path=db_path,
        date=date,
        base_currency=normalized_currency,
        quote_currency="CNY",
        allow_nearest=allow_nearest,
    )


def _query_fx_rate(
    conn: sqlite3.Connection,
    *,
    date: str | None,
    base_currency: str,
    quote_currency: str,
    allow_nearest: bool,
) -> float | None:
    """在汇率表查询单方向汇率记录。"""
    if date is None:
        row = conn.execute(
            """
            SELECT rate
            FROM fx_rates
            WHERE base_currency = ? AND quote_currency = ?
            ORDER BY date DESC
            LIMIT 1;
            """,
            (base_currency, quote_currency),
        ).fetchone()
    elif allow_nearest:
        row = conn.execute(
            """
            SELECT rate
            FROM fx_rates
            WHERE base_currency = ? AND quote_currency = ? AND date <= ?
            ORDER BY date DESC
            LIMIT 1;
            """,
            (base_currency, quote_currency, date),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT rate
            FROM fx_rates
            WHERE base_currency = ? AND quote_currency = ? AND date = ?
            LIMIT 1;
            """,
            (base_currency, quote_currency, date),
        ).fetchone()

    if row is None:
        return None
    return float(row[0])
