import os
import base64
import hashlib
from io import BytesIO
from contextlib import contextmanager
from datetime import datetime

import pandas as pd
import requests
import sqlite3
import altair as alt
import streamlit as st

# =====================================================
# CONFIGURAÇÕES E ESTADO DA SESSÃO
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

if 'comparacao_realizada' not in st.session_state:
    st.session_state.comparacao_realizada = False

# =====================================================
# CONEXÃO COM SQLITE (cacheada)
# =====================================================
@st.cache_resource
def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False, timeout=20)

@contextmanager
def gerenciar_db():
    con = get_connection()
    try:
        yield con
        con.commit()
    except Exception as e:
        con.rollback()
        raise e

# =====================================================
# UTILITÁRIOS
# =====================================================
def to_float(v):
    if pd.isna(v) or v == "":
        return 0.0
    if isinstance(v, str):
        v = v.replace(".", "").replace(",", ".").strip()
    try:
        return float(v)
    except ValueError:
        return 0.0

def gerar_hash_arquivo(uploaded_file):
    uploaded_file.seek(0)
    h = hashlib.sha256(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h

def extrair_valor(row, df, col_opts):
    for c in col_opts:
        if c in df.columns:
            return to_float(row[c])
    return 0.0

# =====================================================
# GITHUB – PERSISTÊNCIA
# =====================================================
def github_request(method, url, **kwargs):
    headers = {
        "Authorization": f"token {st.secrets['GITHUB_TOKEN']}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        r = requests.request(method, url, headers=headers, **kwargs)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        st.error(f"Erro GitHub: {e}")
        return None

def baixar_banco():
    if os.path.exists(DB_NAME):
        return
    try:
        url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        r = github_request("GET", url)
        if r and r.status_code == 200:
            content = r.json()["content"]
            with open(DB_NAME, "wb") as f:
                f.write(base64.b64decode(content))
        else:
            open(DB_NAME, "wb").close()
    except Exception as e:
        st.error(f"Erro ao baixar banco: {e}")

def salvar_banco_github(msg):
    try:
        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        api_url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        r = github_request("GET", api_url)
        sha = r.json().get("sha") if r and r.status_code == 200 else None
        payload = {"message": msg, "content": content, "branch": st.secrets["GITHUB_BRANCH"]}
        if sha:
            payload["sha"] = sha
        github_request("PUT", api_url, json=payload)
    except:
        st.warning("Erro na sincronização com GitHub.")

# =====================================================
# BANCO DE DADOS
# =====================================================
def criar_tabelas():
    with gerenciar_db() as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS procedimentos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT,
                descricao TEXT,
                porte REAL,
                uco REAL,
                filme REAL,
                versao TEXT,
                UNIQUE (codigo, versao)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS arquivos_importados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                versao TEXT,
                data TEXT
            )
        """)

def arquivo_ja_importado(h):
    with get_connection() as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM arquivos_importados WHERE hash=?", (h,))
        return cur.fetchone() is not None

def registrar_arquivo(h, versao):
    with gerenciar_db() as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO arquivos_importados VALUES (NULL,?,?,?)", (h, versao, datetime.now().isoformat()))

def excluir_versao(versao):
    with gerenciar_db() as con:
        cur = con.cursor()
        cur.execute("DELETE FROM procedimentos WHERE versao=?", (versao,))
        total = cur.rowcount
        cur.execute("DELETE FROM arquivos_importados WHERE versao=?", (versao,))
        salvar_banco_github(f"Exclusão da versão {versao}")
        return total

# =====================================================
# IMPORTAÇÃO (CORRIGIDA PARA ERRO DE ENCODING)
# =====================================================
def importar(arquivos, versao):
    if not versao:
        st.error("Por favor, informe a Versão CBHPM.")
        return False

    mapa = {
        "codigo": ["Código", "Codigo"],
        "descricao": ["Descrição", "Descricao"],
        "porte": ["Porte"],
        "uco": ["UCO", "CH"],
        "filme": ["Filme"]
    }

    arquivos_processados = 0
    with gerenciar_db() as con:
        cur = con.cursor()
        for arq in arquivos:
            h = gerar_hash_arquivo(arq)
            if arquivo_ja_importado(h):
                st.warning(f"O conteúdo de '{arq.name}' já foi importado.")
                continue

            # --- CORREÇÃO DE ENCODING AQUI ---
            try:
                if arq.name.lower().endswith(".csv"):
                    try:
                        # Tenta UTF-8 primeiro
                        df = pd.read_csv(arq, sep=";", encoding="utf-8")
                    except UnicodeDecodeError:
                        # Se falhar, tenta Latin-1 (comum em arquivos BR)
                        arq.seek(0)
                        df = pd.read_csv(arq, sep=";", encoding="latin-1")
                else:
                    df = pd.read_excel(arq)
            except Exception as e:
                st.error(f"Erro ao ler o arquivo {arq.name}: {e}")
                continue
            # ---------------------------------

            df.columns = [c.strip() for c in df.columns]

            dados_lista = []
            for _, row in df.iterrows():
                d = {campo: extrair_valor(row, df, cols) for campo, cols in mapa.items()}
                codigo = str(row[next((c for c in mapa["codigo"] if c in df.columns))])
                descricao = str(row[next((c for c in mapa["descricao"] if c in df.columns))])
                dados_lista.append((codigo, descricao, d["porte"], d["uco"], d["filme"], versao))

            cur.executemany(
                "INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao) VALUES (?, ?, ?, ?, ?, ?)",
                dados_lista
            )
            cur.execute(
                "INSERT OR IGNORE INTO arquivos_importados (hash, versao, data) VALUES (?, ?, ?)",
                (h, versao, datetime.now().isoformat())
            )
            arquivos_processados += 1

    if arquivos_processados > 0:
        salvar_banco_github(f"Importação {versao}")
        return True

    return False

# =====================================================
# CONSULTAS
# =====================================================
@st.cache_data
def versoes():
    with get_connection() as con:
        try:
            return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except:
            return []

def buscar_dados(termo, versao, tipo):
    campo = "codigo" if tipo == "Código" else "descricao"
    with get_connection() as con:
        return pd.read_sql(
            f"SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE {campo} LIKE ? AND versao = ?",
            con,
            params=(f"%{termo}%", versao)
        )

# =====================================================
# INTERFACE STREAMLIT
# =====================================================
baixar_banco()
criar_tabelas()

st.set_page_config(page_title="CBHPM Gestão", layout="wide")
st.title("CBHPM • Gestão Inteligente")

lista_versoes = versoes()
v_selecionada = st.sidebar.selectbox("Tabela CBHPM Ativa", lista_versoes, key="v_global") if lista_versoes else None
abas = st.tabs(["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar"])

# --- 1. IMPORTAR ---
with abas[0]:
    versao_importacao = st.text_input("Nome da Versão (ex: CBHPM 2024)", key="txt_v_imp")
    arquivos = st.file_uploader("Upload arquivos", accept_multiple_files=True, key="file_up_imp")
    if st.button("Executar Importação", key="btn_importar_final"):
        if importar(arquivos, versao_importacao):
            st.success(f"Tabela '{versao_importacao}' importada!")
            st.balloons()
            st.cache_data.clear()

# --- 2. CONSULTAR ---
with abas[1]:
    if v_selecionada:
        st.info(f"Versão Ativa na Sidebar: {v_selecionada}")
        c1, c2 = st.columns([1, 3])
        tipo = c1.radio("Tipo", ["Código", "Descrição"], key="radio_tipo_busca")
        termo = c2.text_input("Termo", key="input_termo_busca")
        if st.button("Buscar", key="btn_buscar_consulta"):
            st.dataframe(buscar_dados(termo, v_selecionada, tipo), use_container_width=True)

# --- 3. CALCULAR ---
with abas[2]:
    if v_selecionada:
        cod_calc = st.text_input("Código", key="input_cod_calc")
        col1, col2, col3 = st.columns(3)
        uco_v = col1.number_input("Valor UCO", 1.0, key="input_uco")
        filme_v = col2.number_input("Valor Filme", 21.70, key="input_filme")
        infla_v = col3.number_input("Acréscimo %", 0.0, key="input_infla")
        if st.button("Calcular", key="btn_calcular_total"):
            res = buscar_dados(cod_calc, v_selecionada, "Código")
            if not res.empty:
                p = res.iloc[0]
                f = 1 + (infla_v/100)
                tot = (p['porte']*f) + (p['uco']*uco_v*f) + (p['filme']*filme_v*f)
                st.metric(p['descricao'], f"R$ {tot:,.2f}")

# --- 4. COMPARAR ---
with abas[3]:
    if len(lista_versoes) >= 2:
        col_v1, col_v2 = st.columns(2)
        va = col_v1.selectbox("Base (Antiga)", lista_versoes, key="va_comp",
                              on_change=lambda: st.session_state.update({"comparacao_realizada": False}))
        vb = col_v2.selectbox("Comparação (Nova)", lista_versoes, key="vb_comp",
                              on_change=lambda: st.session_state.update({"comparacao_realizada": False}))
        
        if st.button("Analisar Diferenças", key="btn_analisar_comp"):
            st.session_state.comparacao_realizada = True
            
        if st.session_state.comparacao_realizada:
            dfa = buscar_dados("", va, "Código")
            dfb = buscar_dados("", vb, "Código").rename(columns={
                "porte": "porte_B",
                "uco": "uco_B",
                "filme": "filme_B",
                "descricao": "descricao_B"
            })
            comp = dfa.merge(dfb, on="codigo")
            if not comp.empty:
                comp['perc_var'] = comp.apply(
                    lambda row: ((row['porte_B'] - row['porte']) / row['porte'] * 100) if row['porte'] != 0 else 0,
                    axis=1
                )

                m1, m2, m3 = st.columns(3)
                m1.metric("Itens em Comum", len(comp))
                m2.metric("Variação Média", f"{comp['perc_var'].mean():.2f}%")
                m3.metric("Com Aumento", len(comp[comp['perc_var'] > 0]))
                
                comp['Grupo'] = comp['codigo'].astype(str).str[:2]
                resumo = comp.groupby('Grupo')['perc_var'].mean().reset_index()
                chart = alt.Chart(resumo).mark_bar().encode(
                    x=alt.X('Grupo:N', sort='-y', title="Grupo"),
                    y=alt.Y('perc_var:Q', title="Variação %"),
                    color=alt.condition(alt.datum.perc_var > 0, alt.value('steelblue'), alt.value('orange'))
                ).properties(height=350)
                st.altair_chart(chart, use_container_width=True)
                st.dataframe(comp[['codigo', 'descricao', 'porte', 'porte_B', 'perc_var']], use_container_width=True)
            else:
                st.warning("Nenhuma coincidência encontrada.")

# --- 5. EXPORTAR ---
with abas[4]:
    if st.button("Gerar Arquivo Excel", key="btn_export_xlsx"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            with get_connection() as con:
                pd.read_sql("SELECT * FROM procedimentos", con).to_excel(writer, index=False)
        st.download_button("Clique aqui para baixar", output.getvalue(), "cbhpm_export.xlsx", key="dl_btn")

# --- 6. GERENCIAR ---
with abas[5]:
    if lista_versoes:
        v_del = st.selectbox("Versão para Deletar", lista_versoes, key="v_del_aba")
        if st.button("Confirmar Exclusão Definitiva"):
            excluir_versao(v_del)
            st.cache_data.clear()
            st.rerun()
