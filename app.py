import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
from io import BytesIO
import os
import requests
import base64

# =====================================================
# CONFIG
# =====================================================
DB_NAME = "cbhpm_database.db"

# =====================================================
# GITHUB – PERSISTÊNCIA (GAMBIARRA CONTROLADA)
# =====================================================
def baixar_banco():
    if os.path.exists(DB_NAME):
        return

    url = f"https://raw.githubusercontent.com/{st.secrets['GITHUB_REPO']}/{st.secrets['GITHUB_BRANCH']}/data/{DB_NAME}"

    r = requests.get(
        url,
        headers={"Authorization": f"token {st.secrets['GITHUB_TOKEN']}"}
    )

    if r.status_code == 200:
        with open(DB_NAME, "wb") as f:
            f.write(r.content)
    else:
        open(DB_NAME, "wb").close()

def salvar_banco_github(msg="Atualização automática do banco CBHPM"):
    with open(DB_NAME, "rb") as f:
        content = base64.b64encode(f.read()).decode()

    api_url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/data/{DB_NAME}"

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

# ⬇️ baixa o banco ANTES de tudo
baixar_banco()

# =====================================================
# BANCO
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
    salvar_banco_github(f"Importação CBHPM {versao}")

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
# CÁLCULO
# =====================================================
def calcular(codigo, versao, inflator, valor_filme):
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

# =====================================================
# COMPARAÇÃO
# =====================================================
def comparar(v1, v2):
    df1 = buscar_codigo("", v1)
    df2 = buscar_codigo("", v2).rename(
        columns={"porte": "porte_2", "uco": "uco_2", "filme": "filme_2"}
    )

    df = df1.merge(df2, on="codigo")
    df["Δ Porte"] = df["porte_2"] - df["porte"]
    df["Δ UCO"] = df["uco_2"] - df["uco"]
    df["Δ Filme"] = df["filme_2"] - df["filme"]

    return df

# =====================================================
# EXPORTAÇÃO
# =====================================================
def exportar_excel_por_versao(versoes_selecionadas=None):
    todas = versoes()
    if not versoes_selecionadas:
        versoes_selecionadas = todas

    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for v in versoes_selecionadas:
            df = pd.read_sql("""
                SELECT codigo, descricao, porte, uco, filme
                FROM procedimentos
                WHERE versao=?
            """, conn(), params=(v,))
            if not df.empty:
                df.to_excel(writer, sheet_name=v[:31], index=False)

    output.seek(0)
    return output

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
        st.success("Importação concluída e salva no GitHub")

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
        r = calcular(codigo, v, inflator, valor_filme)
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
        st.dataframe(comparar(v1, v2), use_container_width=True)

# =====================================================
# EXPORTAR
# =====================================================
if menu == "📤 Exportar Excel":
    selecionadas = st.multiselect(
        "Selecione as versões (se não escolher nenhuma, exporta todas)",
        versoes()
    )

    if st.button("Gerar Excel"):
        arquivo = exportar_excel_por_versao(selecionadas)

        st.download_button(
            "⬇️ Baixar Excel",
            data=arquivo,
            file_name="CBHPM_exportacao.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
