import streamlit as st
import pandas as pd
import sqlite3
import os

DB_NAME = "cbhpm_database.db"

# ---------------------------
# BANCO DE DADOS
# ---------------------------
def get_conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def criar_tabela():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS procedimentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT,
            descricao TEXT,
            porte REAL,
            uco REAL,
            filme REAL,
            versao TEXT
        )
    """)
    conn.commit()
    conn.close()

def limpar_tabela():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM procedimentos")
    conn.commit()
    conn.close()

# ---------------------------
# IMPORTAÇÃO
# ---------------------------
def importar_csvs(arquivos):
    conn = get_conn()

    for arquivo in arquivos:
        try:
            df = pd.read_csv(
                arquivo,
                encoding="latin-1",  # evita erro de acentuação
                sep=","
            )

            # Ajuste de colunas conforme modelo CBHPM
            df = df[['Código', 'Descrição', 'Porte', 'UCO', 'Filme']]
            df['versao'] = arquivo.name

            df.columns = [
                'codigo',
                'descricao',
                'porte',
                'uco',
                'filme',
                'versao'
            ]

            df.to_sql(
                'procedimentos',
                conn,
                if_exists='append',
                index=False
            )

        except Exception as e:
            st.error(f"Erro ao importar {arquivo.name}: {e}")

    conn.close()

# ---------------------------
# CONSULTA
# ---------------------------
def consultar_dados(codigo, descricao):
    conn = get_conn()

    query = "SELECT codigo, descricao, porte, uco, filme, versao FROM procedimentos WHERE 1=1"
    params = []

    if codigo:
        query += " AND codigo LIKE ?"
        params.append(f"%{codigo}%")

    if descricao:
        query += " AND descricao LIKE ?"
        params.append(f"%{descricao}%")

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# ---------------------------
# INTERFACE
# ---------------------------
st.set_page_config(page_title="CBHPM App", layout="wide")
st.title("📘 CBHPM – Banco de Dados e Consulta")

criar_tabela()

aba = st.sidebar.radio(
    "Menu",
    ["📥 Importar CBHPM", "🔍 Consultar Procedimentos"]
)

# ---------------------------
# ABA IMPORTAÇÃO
# ---------------------------
if aba == "📥 Importar CBHPM":
    st.subheader("Importar arquivos CSV da CBHPM")

    arquivos = st.file_uploader(
        "Selecione os CSVs",
        type="csv",
        accept_multiple_files=True
    )

    col1, col2 = st.columns(2)

    with col1:
        if st.button("🚀 Importar dados"):
            if arquivos:
                importar_csvs(arquivos)
                st.success("Importação concluída com sucesso!")
            else:
                st.warning("Selecione pelo menos um arquivo CSV.")

    with col2:
        if st.button("🧹 Limpar banco"):
            limpar_tabela()
            st.success("Banco de dados limpo!")

# ---------------------------
# ABA CONSULTA
# ---------------------------
if aba == "🔍 Consultar Procedimentos":
    st.subheader("Consulta de Procedimentos CBHPM")

    col1, col2 = st.columns(2)

    with col1:
        codigo = st.text_input("Código")

    with col2:
        descricao = st.text_input("Descrição")

    if st.button("🔎 Pesquisar"):
        df = consultar_dados(codigo, descricao)

        if df.empty:
            st.warning("Nenhum resultado encontrado.")
        else:
            st.success(f"{len(df)} registros encontrados")
            st.dataframe(df, use_container_width=True)

            # Download
            st.download_button(
                "⬇️ Baixar resultado (CSV)",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="resultado_cbhpm.csv",
                mime="text/csv"
            )
