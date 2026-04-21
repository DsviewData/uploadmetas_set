from __future__ import annotations

import hashlib
import io
import time

import pandas as pd
import streamlit as st

from etl_metas_engine import (
    ETLError,
    build_dataframe_from_header,
    detect_header_row,
    infer_schema,
    read_excel_preview,
    run_import,
    test_connection,
)

st.set_page_config(page_title="ETL Metas Sankhya", page_icon="📈", layout="wide")

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700;800&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.block-container { padding-top: 1.15rem; padding-bottom: 2rem; max-width: 1280px; }
.app-shell { background: linear-gradient(180deg, #0f172a 0%, #111827 100%); color: white; border-radius: 22px; padding: 1.25rem 1.4rem; box-shadow: 0 10px 35px rgba(15, 23, 42, .25); margin-bottom: 1rem; }
.app-title { font-size: 1.85rem; font-weight: 800; margin: 0; color: #f8fafc; }
.app-sub { color: #cbd5e1; margin-top: .35rem; margin-bottom: 0; }
.pill { display: inline-flex; align-items: center; gap: .45rem; border-radius: 999px; padding: .35rem .8rem; font-size: .88rem; font-weight: 700; margin-top: .75rem; width: fit-content; }
.pill-ok { background: rgba(34,197,94,.15); color: #bbf7d0; border: 1px solid rgba(34,197,94,.28); }
.pill-err { background: rgba(239,68,68,.15); color: #fecaca; border: 1px solid rgba(239,68,68,.28); }
.card { border: 1px solid rgba(148,163,184,.22); border-radius: 22px; padding: 1rem 1.1rem; background: #ffffff; box-shadow: 0 10px 25px rgba(2,6,23,.05); margin-bottom: 1rem; }
.section-label { font-size: .95rem; font-weight: 800; color: #0f172a; margin-bottom: .55rem; }
.console-box { background: #020617; color: #e2e8f0; border-radius: 16px; padding: .9rem 1rem; border: 1px solid #1e293b; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .86rem; min-height: 180px; max-height: 320px; overflow-y: auto; white-space: pre-wrap; }
.metric-card { border: 1px solid rgba(148,163,184,.2); border-radius: 18px; padding: .9rem 1rem; background: linear-gradient(180deg, #fff 0%, #f8fafc 100%); }
.metric-label { color: #64748b; font-size: .82rem; font-weight: 700; }
.metric-value { color: #0f172a; font-size: 1.35rem; font-weight: 800; }
.small-muted { color: #64748b; font-size: .84rem; }
.stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p { font-size: .98rem; font-weight: 800; }
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)


def get_postgres_secrets() -> dict:
    if "postgres" not in st.secrets:
        raise ETLError("Credenciais não encontradas em st.secrets['postgres'].")
    return dict(st.secrets["postgres"])


@st.cache_data(ttl=60, show_spinner=False)
def _cached_test_connection(host: str, port: int, dbname: str, user: str, password: str) -> tuple[bool, str]:
    return test_connection({"host": host, "port": port, "dbname": dbname, "user": user, "password": password})


def render_header() -> dict:
    secrets = get_postgres_secrets()
    ok, message = _cached_test_connection(
        host=secrets["host"],
        port=int(secrets["port"]),
        dbname=secrets["dbname"],
        user=secrets["user"],
        password=secrets["password"],
    )
    badge_cls = "pill-ok" if ok else "pill-err"
    badge_text = "Banco conectado" if ok else "Falha de conexão"
    st.markdown(
        f"""
        <div class="app-shell">
            <p class="app-title">ETL Metas Sankhya</p>
            <p class="app-sub">Upload de metas por Time e por Fornecedor com carga completa no PostgreSQL.</p>
            <div class="pill {badge_cls}">{badge_text} · {message}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    return secrets


def render_metrics(result):
    c1, c2, c3, c4 = st.columns(4)
    metrics = [
        (c1, "Linhas lidas", result.total_lido),
        (c2, "Linhas importadas", result.total_importado),
        (c3, "Erros", result.erros),
        (c4, "Duração (s)", f"{result.duracao_segundos:.2f}"),
    ]
    for col, label, value in metrics:
        with col:
            st.markdown(
                f"<div class='metric-card'><div class='metric-label'>{label}</div><div class='metric-value'>{value}</div></div>",
                unsafe_allow_html=True,
            )


def upload_workflow(tab_label: str, table_name: str, secrets: dict):
    key_prefix = table_name
    st.markdown(
        f"<div class='card'><div class='section-label'>{tab_label}</div><div class='small-muted'>Tabela destino: public.{table_name}</div></div>",
        unsafe_allow_html=True,
    )

    uploaded = st.file_uploader(
        f"Selecione o arquivo Excel (.xlsx) — {tab_label}",
        type=["xlsx"],
        key=f"file_{key_prefix}",
    )

    if not uploaded:
        st.info("Envie um arquivo .xlsx para visualizar o preview e confirmar a importação.")
        return

    raw_bytes = uploaded.getvalue()
    try:
        df_raw, sheet_name = read_excel_preview(io.BytesIO(raw_bytes))
    except ETLError as exc:
        st.error(str(exc))
        return

    # Keys tied to file content hash so uploading a different file resets header detection
    file_hash = hashlib.md5(raw_bytes).hexdigest()[:12]
    header_key = f"header_{key_prefix}_{file_hash}"
    header_input_key = f"header_input_{key_prefix}_{file_hash}"

    if header_key not in st.session_state:
        st.session_state[header_key] = detect_header_row(df_raw)

    with st.container(border=True):
        c1, c2 = st.columns([1, 2])
        with c1:
            header_row_ui = st.number_input(
                "Linha do cabeçalho",
                min_value=1,
                max_value=max(1, len(df_raw)),
                value=int(st.session_state[header_key]) + 1,
                step=1,
                key=header_input_key,
                help="Ajuste caso o cabeçalho detectado automaticamente não esteja correto.",
            )
        with c2:
            st.markdown(f"**Aba lida:** `{sheet_name}`")
            st.markdown(f"**Arquivo:** `{uploaded.name}`")

    header_row = int(header_row_ui) - 1
    try:
        df = build_dataframe_from_header(df_raw, header_row)
        schema_map = infer_schema(df)
    except Exception as exc:
        st.error(f"Falha ao montar preview: {exc}")
        return

    if df.empty:
        st.warning("Nenhuma linha útil foi encontrada abaixo do cabeçalho selecionado.")
        return

    left, right = st.columns([1.8, 1.2])
    with left:
        with st.container(border=True):
            st.markdown("<div class='section-label'>Preview (5 linhas)</div>", unsafe_allow_html=True)
            st.dataframe(df.head(5), use_container_width=True, hide_index=True)
    with right:
        with st.container(border=True):
            st.markdown("<div class='section-label'>Tipagem inferida</div>", unsafe_allow_html=True)
            schema_df = pd.DataFrame(schema_map, columns=["coluna", "tipo_postgres"])
            st.dataframe(schema_df, use_container_width=True, hide_index=True)

    if st.button("Confirmar e Importar", type="primary", key=f"import_{key_prefix}"):
        progress = st.progress(0)
        console_placeholder = st.empty()
        logs: list[str] = []

        def log(msg: str):
            ts = time.strftime("%H:%M:%S")
            logs.append(f"[{ts}] {msg}")
            console_placeholder.markdown(
                f"<div class='console-box'>{'<br>'.join(logs)}</div>",
                unsafe_allow_html=True,
            )

        def set_progress(value: float):
            progress.progress(max(0, min(100, int(value * 100))))

        try:
            result = run_import(
                db=secrets,
                df=df,
                schema_name="public",
                table_name=table_name,
                log=log,
                set_progress=set_progress,
            )
            st.success("Importação concluída com sucesso.")
            render_metrics(result)
        except ETLError as exc:
            st.error(f"Falha na importação: {exc}")


def main():
    secrets = render_header()
    tab1, tab2 = st.tabs(["Metas por Time", "Metas por Fornecedor"])
    with tab1:
        upload_workflow("Upload de Metas por Time", "bi_metas_time", secrets)
    with tab2:
        upload_workflow("Upload de Metas por Fornecedor", "bi_metas_fornecedor", secrets)


if __name__ == "__main__":
    main()
