from __future__ import annotations

import io
import math
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Callable

import pandas as pd
import psycopg
from psycopg import sql


class ETLError(Exception):
    pass


@dataclass
class ImportResult:
    total_lido: int
    total_importado: int
    erros: int
    duracao_segundos: float


LogFn = Callable[[str], None]
ProgressFn = Callable[[float], None]


TEXT_HINTS_NUMERIC_4 = ("valor", "total", "meta", "vlr")
TEXT_HINTS_NUMERIC_6 = ("qtd", "quantidade")
TEXT_HINTS_DATE = ("data", "dt_", "date")
TEXT_HINTS_BIGINT = ("cod_", "id_", "codigo")


# Required by project convention
USE_PIPELINE = False


def _is_null_value(x) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and math.isnan(x):
        return True
    return False


def get_connection(db: dict):
    return psycopg.connect(
        host=db["host"],
        port=int(db["port"]),
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
        sslmode=db.get("sslmode", "prefer"),
        connect_timeout=int(db.get("connect_timeout", 10)),
        autocommit=False,
    )


def test_connection(db: dict) -> tuple[bool, str]:
    try:
        with get_connection(db) as conn:
            with conn.cursor() as cur:
                cur.execute("select current_database(), current_schema(), version()")
                row = cur.fetchone()
                return True, f"{row[0]} · schema {row[1]}"
    except Exception as exc:
        return False, str(exc)


def _strip_bom(value: str) -> str:
    return value.replace("\ufeff", "").strip()


def normalize_text_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value)
    text = _strip_bom(text)
    text = re.sub(r"\s+", " ", text)
    return text or None


def snake_case(name: str) -> str:
    text = normalize_text_value(name) or "coluna"
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("%", " pct ")
    text = re.sub(r"[^0-9a-zA-Z_]+", "_", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    if not text:
        text = "coluna"
    if re.match(r"^[0-9]", text):
        text = f"col_{text}"
    return text


def make_unique_column_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique = []
    for name in names:
        base = snake_case(name)
        idx = seen.get(base, 0)
        if idx == 0:
            unique.append(base)
        else:
            unique.append(f"{base}_{idx}")
        seen[base] = idx + 1
    return unique


def read_excel_preview(file_obj: io.BytesIO) -> tuple[pd.DataFrame, str]:
    try:
        xl = pd.ExcelFile(file_obj)
    except Exception as exc:
        raise ETLError(f"Falha ao abrir o Excel: {exc}") from exc

    best_sheet = None
    best_rows = -1
    best_df = None
    for sheet in xl.sheet_names:
        try:
            tmp = pd.read_excel(xl, sheet_name=sheet, header=None, dtype=object)
        except Exception:
            continue
        non_empty_rows = int(tmp.notna().any(axis=1).sum())
        if non_empty_rows > best_rows:
            best_rows = non_empty_rows
            best_sheet = sheet
            best_df = tmp

    if best_df is None or best_sheet is None:
        raise ETLError("Nenhuma aba válida foi encontrada no arquivo.")

    return best_df, best_sheet


def detect_header_row(df_raw: pd.DataFrame) -> int:
    max_rows = min(len(df_raw), 15)
    best_idx = 0
    best_score = -1
    for idx in range(max_rows):
        row = df_raw.iloc[idx].tolist()
        score = 0
        filled = 0
        for cell in row:
            text = normalize_text_value(cell)
            if text:
                filled += 1
                if re.search(r"[A-Za-zÀ-ÿ]", text):
                    score += 2
                if len(text) <= 40:
                    score += 1
                if text.strip().lower() in {"data", "codigo", "codigo_time", "cod_fornecedor", "valor_meta", "equipe"}:
                    score += 3
        score += filled
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx


def build_dataframe_from_header(df_raw: pd.DataFrame, header_row: int) -> pd.DataFrame:
    if header_row >= len(df_raw):
        raise ETLError("Linha de cabeçalho fora do intervalo do arquivo.")

    headers = [normalize_text_value(x) or f"coluna_{i+1}" for i, x in enumerate(df_raw.iloc[header_row].tolist())]
    headers = make_unique_column_names(headers)

    data = df_raw.iloc[header_row + 1 :].copy().reset_index(drop=True)
    data.columns = headers

    data = data.dropna(axis=1, how="all")
    data = data.loc[~data.isna().all(axis=1)].copy()

    for col in data.columns:
        data[col] = data[col].apply(clean_cell_value)

    return data.reset_index(drop=True)


def clean_cell_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        value = normalize_text_value(value)
        return value
    return value


def _name_has_any(name: str, hints: tuple[str, ...]) -> bool:
    name = name.lower()
    return any(h in name for h in hints)


def _series_non_null(series: pd.Series) -> pd.Series:
    return series.dropna().reset_index(drop=True)


def _parse_decimal(text: str):
    text = normalize_text_value(text)
    if text is None:
        return None
    text = text.replace("R$", "").replace(" ", "")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _can_be_integer_series(series: pd.Series) -> bool:
    s = _series_non_null(series)
    if s.empty:
        return False
    for value in s:
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            continue
        if isinstance(value, float):
            if not value.is_integer():
                return False
            continue
        if isinstance(value, Decimal):
            if value != value.to_integral_value():
                return False
            continue
        parsed = _parse_decimal(str(value))
        if parsed is None or parsed != parsed.to_integral_value():
            return False
    return True


def _coerce_date_series(series: pd.Series) -> list[date | None] | None:
    out = []
    for value in series:
        if _is_null_value(value):
            out.append(None)
            continue
        if isinstance(value, pd.Timestamp):
            out.append(value.date())
            continue
        if isinstance(value, datetime):
            out.append(value.date())
            continue
        if isinstance(value, date):
            out.append(value)
            continue
        parsed = pd.to_datetime(str(value), errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return None
        out.append(parsed.date())
    return out


def infer_column_type(col_name: str, series: pd.Series) -> str:
    non_null = _series_non_null(series)
    max_len = int(non_null.astype(str).map(len).max()) if not non_null.empty else 0

    if _name_has_any(col_name, TEXT_HINTS_DATE):
        parsed = _coerce_date_series(series)
        if parsed is not None:
            return "DATE"

    if _name_has_any(col_name, TEXT_HINTS_BIGINT) and _can_be_integer_series(series):
        return "BIGINT"

    if _name_has_any(col_name, TEXT_HINTS_NUMERIC_4):
        return "NUMERIC(15,4)"

    if _name_has_any(col_name, TEXT_HINTS_NUMERIC_6):
        return "NUMERIC(15,6)"

    if max_len and max_len <= 10:
        return "VARCHAR(20)"

    return "TEXT"


def infer_schema(df: pd.DataFrame) -> list[tuple[str, str]]:
    return [(col, infer_column_type(col, df[col])) for col in df.columns]


def cast_dataframe_to_schema(df: pd.DataFrame, schema_map: list[tuple[str, str]]) -> pd.DataFrame:
    out = df.copy()
    for col, pg_type in schema_map:
        if pg_type == "DATE":
            out[col] = pd.to_datetime(out[col], errors="coerce", dayfirst=True).dt.date
        elif pg_type.startswith("NUMERIC"):
            def _to_decimal(x):
                if _is_null_value(x):
                    return None
                if isinstance(x, Decimal):
                    return x
                if isinstance(x, str):
                    return _parse_decimal(x)
                return Decimal(str(x))
            out[col] = out[col].apply(_to_decimal)
        elif pg_type == "BIGINT":
            def _to_bigint(v):
                if _is_null_value(v):
                    return None
                if isinstance(v, int):
                    return v
                if isinstance(v, float):
                    return int(v)
                if isinstance(v, Decimal):
                    return int(v)
                parsed = _parse_decimal(str(v))
                return int(parsed) if parsed is not None else None
            out[col] = out[col].apply(_to_bigint)
        else:
            out[col] = out[col].apply(lambda x: None if _is_null_value(x) else normalize_text_value(x))
    return out


def ensure_table(conn, schema_name: str, table_name: str, schema_map: list[tuple[str, str]]):
    with conn.cursor() as cur:
        columns_sql = [sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type)) for col, pg_type in schema_map]
        columns_sql.extend(
            [
                sql.SQL("origem_sistema TEXT NOT NULL DEFAULT 'METAS'"),
                sql.SQL("dt_importacao TIMESTAMP NOT NULL DEFAULT NOW()"),
            ]
        )
        create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(columns_sql),
        )
        cur.execute(create_query)

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema_name, table_name),
        )
        existing = {row[0] for row in cur.fetchall()}
        for col, pg_type in schema_map:
            if col not in existing:
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                        sql.Identifier(schema_name), sql.Identifier(table_name), sql.Identifier(col), sql.SQL(pg_type)
                    )
                )
        if "origem_sistema" not in existing:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN origem_sistema TEXT NOT NULL DEFAULT 'METAS'").format(
                    sql.Identifier(schema_name), sql.Identifier(table_name)
                )
            )
        if "dt_importacao" not in existing:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN dt_importacao TIMESTAMP NOT NULL DEFAULT NOW()").format(
                    sql.Identifier(schema_name), sql.Identifier(table_name)
                )
            )


def truncate_table(conn, schema_name: str, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))
        )


def insert_dataframe(conn, schema_name: str, table_name: str, df: pd.DataFrame):
    if df.empty:
        return 0

    cols = list(df.columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in cols)
    insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
        placeholders,
    )

    rows = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        cur.executemany(insert_query, rows)
    return len(rows)


def run_import(
    db: dict,
    df: pd.DataFrame,
    schema_name: str,
    table_name: str,
    log: LogFn | None = None,
    set_progress: ProgressFn | None = None,
) -> ImportResult:
    start = time.perf_counter()
    log = log or (lambda msg: None)
    set_progress = set_progress or (lambda value: None)

    total_lido = len(df)
    erros = 0

    log("Inferindo schema PostgreSQL...")
    schema_map = infer_schema(df)
    typed_df = cast_dataframe_to_schema(df, schema_map)
    set_progress(0.15)

    log(f"Conectando ao PostgreSQL: {db['host']}:{db['port']}/{db['dbname']}")
    with get_connection(db) as conn:
        try:
            log(f"Garantindo tabela public.{table_name}...")
            ensure_table(conn, schema_name, table_name, schema_map)
            set_progress(0.35)

            log(f"Limpando tabela public.{table_name} (TRUNCATE)...")
            truncate_table(conn, schema_name, table_name)
            set_progress(0.55)

            log("Inserindo registros...")
            total_importado = insert_dataframe(conn, schema_name, table_name, typed_df)
            set_progress(0.9)

            conn.commit()
            log("Commit concluído com sucesso.")
        except Exception as exc:
            conn.rollback()
            erros += 1
            log(f"Erro na importação: {exc}")
            raise ETLError(str(exc)) from exc

    set_progress(1.0)
    duracao = time.perf_counter() - start
    return ImportResult(
        total_lido=total_lido,
        total_importado=total_importado,
        erros=erros,
        duracao_segundos=duracao,
    )
