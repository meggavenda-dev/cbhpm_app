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

# =====================================================
# EXCLUSÃO COM ROLLBACK
# =====================================================
def excluir_versao(versao):
    con = conn()
    try:
        cur = con.cursor()
        cur.execute("BEGIN")

        cur.execute("SELECT COUNT(*) FROM procedimentos WHERE versao=?", (versao,))
        total = cur.fetchone()[0]

        cur.execute("DELETE FROM procedimentos WHERE versao=?", (versao,))
        cur.execute("DELETE FROM arquivos_importados WHERE versao=?", (versao,))

        con.commit()
        salvar_banco_github(f"Exclusão da versão {versao}")
        return total

    except Exception as e:
        con.rollback()
        raise e
    finally:
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

        cur.execute("""
            INSERT OR IGNORE INTO arquivos_importados
            VALUES (NULL,?,?,?)
        """, (h, versao, datetime.now().isoformat()))

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
    "📤 Exportar",
    "🗑️ Excluir versão"
])

# ---------------- EXCLUIR ----------------
with abas[5]:
    v = st.selectbox("Versão CBHPM para exclusão", versoes())
    confirmar = st.checkbox("Confirmo a exclusão definitiva desta versão")

    if st.button("Excluir versão"):
        if not confirmar:
            st.warning("Confirme a exclusão")
        else:
            total = excluir_versao(v)
            st.success(f"Versão {v} excluída. {total} registros removidos.")
