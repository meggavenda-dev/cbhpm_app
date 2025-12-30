import streamlit as st
import pandas as pd
import sqlite3

DB_NAME = "cbhpm_database.db"

# =====================================================
# BANCO DE DADOS
# =====================================================
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
            versao TEXT,
            UNIQUE (codigo, versao)
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

# =====================================================
# IMPORTAÇÃO DE CSV
# =====================================================
def importar_csvs(arquivos):
    conn = get_conn()
    cursor = conn.cursor()

    for arquivo in arquivos:
        try:
            df = pd.read_csv(
                arquivo,
                encoding="latin-1",
                sep=";",
                engine="python",
                on_bad_lines="skip"
            )

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

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, tuple(row))

            conn.commit()

        except Exception as e:
            st.error(f"Erro ao importar {arquivo.name}: {e}")

    conn.close()

# =====================================================
# CONSULTAS
# =====================================================
def listar_versoes():
    conn = get_conn()
    df = pd.read_sql(
        "SELECT DISTINCT versao FROM procedimentos ORDER BY versao",
        conn
    )
    conn.close()
    return df['versao'].tolist()

def consultar_dados(codigo, descricao, versao):
    conn = get_conn()

    query = """
        SELECT codigo, descricao, porte, uco, filme, versao
        FROM procedimentos
        WHERE versao = ?
    """
    params = [versao]

    if codigo:
        query += " AND codigo LIKE ?"
        params.append(f"%{codigo}%")

    if descricao:
        query += " AND descricao LIKE ?"
        params.append(f"%{descricao}%")

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# =====================================================
# INTERFACE STREAMLIT
# =====================================================
st.set_page_config(page_title="CBHPM App", layout="wide")
st.title("📘 CBHPM – Banco de Dados e Consulta")

criar_tabela()

menu = st.sidebar.radio(
    "Menu",
    ["📥 Importar CBHPM", "🔍 Consultar Procedimentos"]
)

# =====================================================
# ABA IMPORTAÇÃO
# =====================================================
if menu == "📥 Importar CBHPM":
    st.subheader("Importar arquivos CSV da CBHPM")

    arquivos = st.file_uploader(
        "Selecione os arquivos CSV",
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
            st.success("Banco de dados limpo com sucesso!")

# =====================================================
# ABA CONSULTA
# =====================================================
if menu == "🔍 Consultar Procedimentos":
    st.subheader("Consulta de Procedimentos CBHPM")

    versoes = listar_versoes()

    if not versoes:
        st.warning("Nenhuma tabela CBHPM importada ainda.")
    else:
        col1, col2, col3 = st.columns(3)

        with col1:
            versao_selecionada = st.selectbox(
                "Tabela CBHPM",
                versoes
            )

        with col2:
            codigo = st.text_input("Código")

        with col3:
            descricao = st.text_input("Descrição")

        if st.button("🔎 Pesquisar"):
            df = consultar_dados(
                codigo=codigo,
                descricao=descricao,
                versao=versao_selecionada
            )

            if df.empty:
                st.warning("Nenhum resultado encontrado para esta tabela.")
            else:
                st.success(f"{len(df)} registros encontrados")
                st.dataframe(df, use_container_width=True)

                st.download_button(
                    "⬇️ Baixar resultado (CSV)",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="resultado_cbhpm.csv",
                    mime="text/csv"
                )
