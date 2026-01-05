import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os
import requests
import base64
import hashlib
from io import BytesIO

# =====================================================
# CONFIGURAÇÕES
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

# =====================================================
# CONEXÃO
# =====================================================
def conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

# =====================================================
# UTIL
# =====================================================
def to_float(v):
    try:
        if pd.isna(v):
            return 0.0
        if isinstance(v, str):
            v = v.replace(",", ".").strip()
        return float(v)
    except:
        return 0.0

def gerar_hash_arquivo(uploaded_file):
    uploaded_file.seek(0)
    h = hashlib.sha256(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h

# =====================================================
# GITHUB – PERSISTÊNCIA
# =====================================================
def baixar_banco():
    if os.path.exists(DB_NAME):
        return
    try:
        url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        headers = {
            "Authorization": f"token {st.secrets['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github.v3+json"
        }
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            content = r.json()["content"]
            with open(DB_NAME, "wb") as f:
                f.write(base64.b64decode(content))
        else:
            open(DB_NAME, "wb").close()
    except Exception as e:
        st.error(e)

def salvar_banco_github(msg):
    with open(DB_NAME, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    api_url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
    headers = {
        "Authorization": f"token {st.secrets['GITHUB_TOKEN']}",
        "Accept": "application/vnd.github.v3+json"
    }

    r = requests.get(api_url, headers=headers)
    sha = r.json().get("sha") if r.status_code == 200 else None

    payload = {
        "message": msg,
        "content": content,
        "branch": st.secrets["GITHUB_BRANCH"]
    }
    if sha:
        payload["sha"] = sha

    requests.put(api_url, headers=headers, json=payload)

# =====================================================
# BANCO
# =====================================================
def criar_tabelas():
    con = conn()
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

    con.commit()
    con.close()

def arquivo_ja_importado(h):
    con = conn()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM arquivos_importados WHERE hash=?", (h,))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def registrar_arquivo(h, versao):
    con = conn()
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO arquivos_importados
        VALUES (NULL,?,?,?)
    """, (h, versao, datetime.now().isoformat()))
    con.commit()
    con.close()

# =====================================================
# IMPORTAÇÃO
# =====================================================
def importar(arquivos, versao):
    mapa = {
        "codigo": ["Código", "Codigo"],
        "descricao": ["Descrição", "Descricao"],
        "porte": ["Porte"],
        "uco": ["UCO", "CH"],
        "filme": ["Filme"]
    }

    con = conn()
    cur = con.cursor()

    for arq in arquivos:
        h = gerar_hash_arquivo(arq)
        if arquivo_ja_importado(h):
            continue

        df = pd.read_excel(arq) if not arq.name.endswith(".csv") else pd.read_csv(arq, sep=";")
        df.columns = [c.strip() for c in df.columns]

        dados = {}
        for campo, cols in mapa.items():
            col = next((c for c in cols if c in df.columns), None)
            dados[campo] = df[col] if col else 0

        df_f = pd.DataFrame(dados)
        df_f["versao"] = versao

        for c in ["porte", "uco", "filme"]:
            df_f[c] = df_f[c].apply(to_float)

        for _, r in df_f.iterrows():
            cur.execute("""
                INSERT OR IGNORE INTO procedimentos
                VALUES (NULL,?,?,?,?,?,?)
            """, tuple(r))

        registrar_arquivo(h, versao)

    con.commit()
    con.close()
    salvar_banco_github(f"Importação {versao}")

# =====================================================
# CONSULTAS
# =====================================================
def versoes():
    return pd.read_sql(
        "SELECT DISTINCT versao FROM procedimentos ORDER BY versao",
        conn()
    )["versao"].tolist()

def buscar_codigo(codigo, versao):
    return pd.read_sql(
        """
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE codigo LIKE ? AND versao = ?
        """,
        conn(),
        params=(f"%{codigo}%", versao)
    )

def buscar_descricao(desc, versao):
    return pd.read_sql(
        """
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE descricao LIKE ? AND versao = ?
        """,
        conn(),
        params=(f"%{desc}%", versao)
    )

# =====================================================
# INICIALIZAÇÃO
# =====================================================
baixar_banco()
criar_tabelas()

# =====================================================
# INTERFACE
# =====================================================
st.title("CBHPM • Gestão Inteligente")

abas = st.tabs([
    "📥 Importar",
    "📋 Consultar",
    "🧮 Calcular",
    "⚖️ Comparar versões",
    "📤 Exportar"
])

# ---------------- IMPORTAR ----------------
with abas[0]:
    versao = st.text_input("Versão CBHPM")
    arquivos = st.file_uploader("Arquivos", accept_multiple_files=True)
    if st.button("Importar dados"):
        importar(arquivos, versao)
        st.success("Importação concluída")

# ---------------- CONSULTAR ----------------
with abas[1]:
    v = st.selectbox("Tabela CBHPM", versoes())
    tipo = st.radio("Buscar por", ["Código", "Descrição"])
    termo = st.text_input("Termo de busca")

    if st.button("Buscar"):
        df = buscar_codigo(termo, v) if tipo == "Código" else buscar_descricao(termo, v)
        st.dataframe(df, use_container_width=True)

# ---------------- CALCULAR ----------------
with abas[2]:
    v = st.selectbox("Tabela CBHPM", versoes())
    codigo = st.text_input("Código do procedimento")

    col1, col2, col3 = st.columns(3)
    valor_uco = col1.number_input("Valor da UCO", value=1.0)
    valor_filme = col2.number_input("Valor do Filme", value=21.70)
    inflator = col3.number_input("Inflator (%)", value=0.0)

    if st.button("Calcular"):
        df = buscar_codigo(codigo, v)
        if df.empty:
            st.warning("Procedimento não encontrado")
        else:
            p = df.iloc[0]
            fator = 1 + inflator / 100

            porte = p["porte"] * fator
            uco = p["uco"] * valor_uco * fator
            filme = p["filme"] * valor_filme * fator
            total = porte + uco + filme

            st.success(p["descricao"])
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Porte", f"R$ {porte:,.2f}")
            c2.metric("UCO", f"R$ {uco:,.2f}")
            c3.metric("Filme", f"R$ {filme:,.2f}")
            c4.metric("💰 Total", f"R$ {total:,.2f}")

# ---------------- COMPARAR ----------------
with abas[3]:
    v1 = st.selectbox("Versão A", versoes())
    v2 = st.selectbox("Versão B", versoes())
    df1 = buscar_codigo("", v1)
    df2 = buscar_codigo("", v2).rename(
        columns={"porte": "porte_B", "uco": "uco_B", "filme": "filme_B"}
    )
    comp = df1.merge(df2, on="codigo")
    st.dataframe(comp, use_container_width=True)

# ---------------- EXPORTAR ----------------
with abas[4]:
    tabelas = ["procedimentos", "arquivos_importados"]
    escolha = st.multiselect("Tabelas", tabelas)

    con = conn()
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        if escolha:
            for t in escolha:
                pd.read_sql(f"SELECT * FROM {t}", con).to_excel(writer, sheet_name=t, index=False)
        else:
            for t in tabelas:
                pd.read_sql(f"SELECT * FROM {t}", con).to_excel(writer, sheet_name=t, index=False)
    con.close()

    st.download_button(
        "Baixar Excel",
        data=output.getvalue(),
        file_name="cbhpm_export.xlsx"
    )
