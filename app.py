import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

DB_NAME = "cbhpm_database.db"

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

# =====================================================
# TABELAS
# =====================================================
def criar_tabelas():
    con = conn()
    c = con.cursor()

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
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS convenios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT UNIQUE,
        inflator REAL,
        valor_filme REAL
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS log_importacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        versao TEXT,
        arquivo TEXT,
        problema TEXT,
        data TEXT
    )
    """)

    con.commit()
    con.close()

# =====================================================
# IMPORTAÇÃO
# =====================================================
def ler_arquivo(arq):
    if arq.name.lower().endswith(".csv"):
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

            for campo, colunas in mapa.items():
                col = next((c for c in colunas if c in df.columns), None)
                if col:
                    dados[campo] = df[col]
                else:
                    dados[campo] = 0.0
                    faltando.append(campo)

            if faltando:
                cur.execute("""
                    INSERT INTO log_importacao
                    (versao, arquivo, problema, data)
                    VALUES (?,?,?,?)
                """, (versao, arq.name, f"Colunas ausentes: {faltando}", datetime.now().isoformat()))

            df_f = pd.DataFrame(dados)
            df_f["versao"] = versao

            for c in ["porte", "uco", "filme"]:
                df_f[c] = df_f[c].apply(to_float)

            for _, r in df_f.iterrows():
                cur.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?,?,?,?,?,?)
                """, tuple(r))

        except Exception as e:
            cur.execute("""
                INSERT INTO log_importacao
                (versao, arquivo, problema, data)
                VALUES (?,?,?,?)
            """, (versao, arq.name, str(e), datetime.now().isoformat()))

    con.commit()
    con.close()

# =====================================================
# CONSULTAS
# =====================================================
def versoes():
    return pd.read_sql(
        "SELECT DISTINCT versao FROM procedimentos ORDER BY versao",
        conn()
    )["versao"].tolist()

def buscar_codigo(codigo, versao):
    return pd.read_sql("""
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE codigo LIKE ? AND versao = ?
    """, conn(), params=(f"%{codigo}%", versao))

def buscar_descricao(desc, versao):
    return pd.read_sql("""
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE descricao LIKE ? AND versao = ?
    """, conn(), params=(f"%{desc}%", versao))

# =====================================================
# CÁLCULOS
# =====================================================
def calcular_manual(codigo, versao, inflator, valor_filme):
    df = buscar_codigo(codigo, versao)
    if df.empty:
        return None

    p = df.iloc[0]
    fator = 1 + inflator / 100

    porte = p["porte"] * fator
    uco = p["uco"] * fator
    filme = p["filme"] * valor_filme

    total = porte + uco + filme
    return p["descricao"], porte, uco, filme, total

def simular_convenio(codigo, versao, convenio):
    con = conn()
    proc = pd.read_sql(
        "SELECT porte, uco, filme FROM procedimentos WHERE codigo=? AND versao=?",
        con, params=(codigo, versao)
    )
    conv = pd.read_sql(
        "SELECT inflator, valor_filme FROM convenios WHERE nome=?",
        con, params=(convenio,)
    )
    con.close()

    if proc.empty or conv.empty:
        return None

    p = proc.iloc[0]
    c = conv.iloc[0]
    fator = 1 + c["inflator"] / 100

    total = (p["porte"] + p["uco"]) * fator + p["filme"] * c["valor_filme"]
    return total

# =====================================================
# EXPORTAÇÃO
# =====================================================
def exportar_excel():
    v = versoes()
    arquivo = "CBHPM_Completa.xlsx"
    with pd.ExcelWriter(arquivo, engine="xlsxwriter") as w:
        for x in v:
            pd.read_sql(
                "SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE versao=?",
                conn(), params=(x,)
            ).to_excel(w, sheet_name=x[:31], index=False)
    return arquivo

# =====================================================
# INTERFACE
# =====================================================
st.set_page_config("CBHPM Profissional", layout="wide")
st.title("📊 Plataforma CBHPM")

criar_tabelas()

menu = st.sidebar.radio("Menu", [
    "📥 Importar",
    "📋 Consultar",
    "🧮 Calcular",
    "🔍 Comparar",
    "📤 Exportar Excel"
])

# =====================================================
# IMPORTAR
# =====================================================
if menu == "📥 Importar":
    versao = st.text_input("Versão CBHPM")
    arquivos = st.file_uploader("CSV ou XLSX", ["csv", "xlsx"], True)
    if st.button("Importar"):
        importar(arquivos, versao)
        st.success("Importação concluída")

# =====================================================
# CONSULTAR
# =====================================================
if menu == "📋 Consultar":
    v = st.selectbox("Versão", versoes())
    tipo = st.radio("Buscar por", ["Código", "Descrição"])
    termo = st.text_input("Termo")

    if st.button("Buscar"):
        df = buscar_codigo(termo, v) if tipo == "Código" else buscar_descricao(termo, v)
        st.dataframe(df, use_container_width=True)

# =====================================================
# CALCULAR
# =====================================================
if menu == "🧮 Calcular":
    v = st.selectbox("Versão", versoes())
    codigo = st.text_input("Código do procedimento")
    inflator = st.number_input("Inflator (%)", 0.0, 500.0, 0.0)
    valor_filme = st.number_input("Valor do filme", 0.0, 1000.0, 21.70)

    if st.button("Calcular"):
        r = calcular_manual(codigo, v, inflator, valor_filme)
        if not r:
            st.warning("Procedimento não encontrado")
        else:
            desc, porte, uco, filme, total = r
            st.info(desc)
            c1, c2, c3 = st.columns(3)
            c1.metric("Porte", f"R$ {porte:,.2f}")
            c2.metric("UCO", f"R$ {uco:,.2f}")
            c3.metric("Filme", f"R$ {filme:,.2f}")
            st.success(f"💰 Total: R$ {total:,.2f}")

# =====================================================
# COMPARAR
# =====================================================
if menu == "🔍 Comparar":
    v1 = st.selectbox("Versão base", versoes())
    v2 = st.selectbox("Versão comparada", versoes())
    if st.button("Comparar"):
        df1 = buscar_codigo("", v1)
        df2 = buscar_codigo("", v2).rename(
            columns={"porte":"porte_2","uco":"uco_2","filme":"filme_2"}
        )
        df = df1.merge(df2, on="codigo")
        df["Δ Porte"] = df["porte_2"] - df["porte"]
        df["Δ UCO"] = df["uco_2"] - df["uco"]
        st.dataframe(df, use_container_width=True)

# =====================================================
# EXPORTAR
# =====================================================
if menu == "📤 Exportar Excel":
    if st.button("Gerar Excel"):
        arq = exportar_excel()
        st.download_button("Download", open(arq,"rb"), file_name=arq)
