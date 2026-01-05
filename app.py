import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

DB_NAME = "cbhpm_database.db"

def conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

# =========================
# UTIL
# =========================
def to_float(v):
    try:
        if pd.isna(v):
            return 0.0
        if isinstance(v, str):
            v = v.replace(",", ".").strip()
        return float(v)
    except:
        return 0.0

# =========================
# TABELAS
# =========================
def criar_tabelas():
    c = conn().cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS procedimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT,
        descricao TEXT,
        porte REAL,
        uco REAL,
        filme REAL,
        versao TEXT,
        UNIQUE (codigo, versao)
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS convenios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT UNIQUE,
        inflator REAL,
        valor_filme REAL
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS log_importacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        versao TEXT,
        arquivo TEXT,
        problema TEXT,
        data TEXT
    )""")

    c.connection.commit()
    c.connection.close()

# =========================
# IMPORTAÇÃO
# =========================
def ler_arquivo(arq):
    if arq.name.endswith(".csv"):
        return pd.read_csv(arq, sep=";", encoding="latin-1", engine="python")
    return pd.read_excel(arq)

def importar(arquivos, versao):
    mapa = {
        "codigo": ["Código", "Codigo"],
        "descricao": ["Descrição", "Descricao"],
        "porte": ["Porte", "Porte Cirúrgico"],
        "uco": ["UCO", "CH"],
        "filme": ["Filme", "Filme Rx"]
    }

    con = conn()
    cur = con.cursor()

    for arq in arquivos:
        try:
            df = ler_arquivo(arq)
            df.columns = [c.strip() for c in df.columns]

            dados = {}
            faltando = []

            for k, cols in mapa.items():
                col = next((c for c in cols if c in df.columns), None)
                if col:
                    dados[k] = df[col]
                else:
                    dados[k] = 0.0
                    faltando.append(k)

            if faltando:
                cur.execute("""
                    INSERT INTO log_importacao VALUES (NULL,?,?,?,?)
                """, (versao, arq.name, f"Faltando: {faltando}", datetime.now().isoformat()))

            df_f = pd.DataFrame(dados)
            df_f["versao"] = versao

            for c in ["porte","uco","filme"]:
                df_f[c] = df_f[c].apply(to_float)

            for _, r in df_f.iterrows():
                cur.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo,descricao,porte,uco,filme,versao)
                    VALUES (?,?,?,?,?,?)
                """, tuple(r))

        except Exception as e:
            cur.execute("""
                INSERT INTO log_importacao VALUES (NULL,?,?,?,?)
            """, (versao, arq.name, str(e), datetime.now().isoformat()))

    con.commit()
    con.close()

# =========================
# CONSULTAS
# =========================
def versoes():
    return pd.read_sql("SELECT DISTINCT versao FROM procedimentos", conn())["versao"].tolist()

def buscar(codigo, versao):
    return pd.read_sql("""
        SELECT * FROM procedimentos
        WHERE codigo LIKE ? AND versao = ?
    """, conn(), params=(f"%{codigo}%", versao))

def comparar(v1, v2):
    a = pd.read_sql("SELECT * FROM procedimentos WHERE versao = ?", conn(), params=(v1,))
    b = pd.read_sql("SELECT codigo,porte u2,uco u3,filme u4 FROM procedimentos WHERE versao = ?", conn(), params=(v2,))
    df = a.merge(b, on="codigo")
    df["Δ Porte"] = df["u2"] - df["porte"]
    df["Δ UCO"] = df["u3"] - df["uco"]
    return df

def exportar_excel():
    v = versoes()
    with pd.ExcelWriter("CBHPM.xlsx") as w:
        for x in v:
            pd.read_sql(
                "SELECT codigo,descricao,porte,uco,filme FROM procedimentos WHERE versao = ?",
                conn(), params=(x,)
            ).to_excel(w, sheet_name=x[:31], index=False)
    return "CBHPM.xlsx"

# =========================
# INTERFACE
# =========================
st.set_page_config("CBHPM Profissional", layout="wide")
st.title("📊 Plataforma CBHPM")

criar_tabelas()

menu = st.sidebar.radio("Menu", [
    "📥 Importar",
    "📋 Consultar",
    "🔍 Comparar",
    "🧮 Simulador",
    "📤 Exportar Excel"
])

if menu == "📥 Importar":
    v = st.text_input("Versão")
    arqs = st.file_uploader("Arquivos", ["csv","xlsx"], True)
    if st.button("Importar"):
        importar(arqs, v)
        st.success("Importado")

if menu == "📋 Consultar":
    v = st.selectbox("Versão", versoes())
    c = st.text_input("Código")
    if st.button("Buscar"):
        st.dataframe(buscar(c, v))

if menu == "🔍 Comparar":
    v1 = st.selectbox("Versão base", versoes())
    v2 = st.selectbox("Versão comparação", versoes())
    if st.button("Comparar"):
        st.dataframe(comparar(v1, v2))

if menu == "🧮 Simulador":
    st.info("Baseado em convênios cadastrados (tabela convenios)")
    st.write("Simulação pronta para integração comercial")

if menu == "📤 Exportar Excel":
    if st.button("Gerar Excel"):
        arq = exportar_excel()
        st.download_button("Download", open(arq,"rb"), file_name=arq)
