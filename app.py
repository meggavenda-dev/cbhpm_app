import streamlit as st
import pandas as pd
import sqlite3
from io import BytesIO

# =====================================================
# CONFIGURAÇÃO
# =====================================================
st.set_page_config(page_title="CBHPM Manager", layout="wide")

DB_NAME = "cbhpm_database.db"


# =====================================================
# CONEXÃO COM BANCO
# =====================================================
def get_connection():
    return sqlite3.connect(DB_NAME)


# =====================================================
# FUNÇÕES UTILITÁRIAS
# =====================================================
def listar_tabelas():
    conn = get_connection()
    query = """
        SELECT name FROM sqlite_master
        WHERE type='table'
        AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """
    tabelas = pd.read_sql(query, conn)["name"].tolist()
    conn.close()
    return tabelas


def carregar_tabela(nome_tabela):
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {nome_tabela}", conn)
    conn.close()
    return df


def to_float(valor):
    try:
        if valor is None:
            return 0.0
        if isinstance(valor, str):
            valor = valor.replace(",", ".").strip()
        return float(valor)
    except:
        return 0.0


# =====================================================
# PESQUISA CBHPM
# =====================================================
def pesquisar_cbpmp(tabela, codigo=None, descricao=None):
    conn = get_connection()

    query = f"SELECT * FROM {tabela} WHERE 1=1"
    params = []

    if codigo:
        query += " AND Codigo LIKE ?"
        params.append(f"%{codigo}%")

    if descricao:
        query += " AND Descricao LIKE ?"
        params.append(f"%{descricao}%")

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# =====================================================
# CÁLCULO DE HONORÁRIOS
# =====================================================
def calcular_honorarios(df, valor_uco, valor_porte):
    df = df.copy()

    df["UCO"] = df["UCO"].apply(to_float)
    df["Porte"] = df["Porte"].apply(to_float)

    df["Valor UCO (R$)"] = df["UCO"] * valor_uco
    df["Valor Porte (R$)"] = df["Porte"] * valor_porte
    df["Honorário Total (R$)"] = df["Valor UCO (R$)"] + df["Valor Porte (R$)"]

    return df


# =====================================================
# EXPORTAÇÃO PARA EXCEL
# =====================================================
def exportar_excel(tabelas_selecionadas=None):
    todas_tabelas = listar_tabelas()

    if not tabelas_selecionadas:
        tabelas_selecionadas = todas_tabelas

    output = BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for tabela in tabelas_selecionadas:
            df = carregar_tabela(tabela)
            df.to_excel(writer, sheet_name=tabela[:31], index=False)

    output.seek(0)
    return output


# =====================================================
# INTERFACE
# =====================================================
st.title("📘 Gerenciador CBHPM")

menu = st.sidebar.radio(
    "Menu",
    [
        "Pesquisar Procedimentos",
        "Simulador de Honorários",
        "Exportar Banco para Excel"
    ]
)

tabelas = listar_tabelas()

# =====================================================
# 🔍 PESQUISA
# =====================================================
if menu == "Pesquisar Procedimentos":
    st.subheader("🔍 Pesquisa CBHPM")

    tabela = st.selectbox("Versão CBHPM", tabelas)
    col1, col2 = st.columns(2)

    with col1:
        codigo = st.text_input("Código do procedimento")

    with col2:
        descricao = st.text_input("Descrição do procedimento")

    if st.button("Pesquisar"):
        resultado = pesquisar_cbpmp(tabela, codigo, descricao)
        st.dataframe(resultado, use_container_width=True)


# =====================================================
# 🧮 SIMULADOR DE HONORÁRIOS
# =====================================================
elif menu == "Simulador de Honorários":
    st.subheader("🧮 Simulador de Honorários")

    tabela = st.selectbox("Versão CBHPM", tabelas)

    col1, col2 = st.columns(2)

    with col1:
        valor_uco = st.number_input("Valor da UCO (R$)", min_value=0.0, value=20.0)

    with col2:
        valor_porte = st.number_input("Valor do Porte (R$)", min_value=0.0, value=100.0)

    codigo = st.text_input("Código do procedimento")
    descricao = st.text_input("Descrição do procedimento")

    if st.button("Calcular"):
        df = pesquisar_cbpmp(tabela, codigo, descricao)

        if df.empty:
            st.warning("Nenhum procedimento encontrado.")
        else:
            df_calc = calcular_honorarios(df, valor_uco, valor_porte)
            st.dataframe(df_calc, use_container_width=True)


# =====================================================
# 📤 EXPORTAR EXCEL
# =====================================================
elif menu == "Exportar Banco para Excel":
    st.subheader("📤 Exportar banco de dados")

    tabelas_selecionadas = st.multiselect(
        "Selecione as tabelas que deseja exportar (se não selecionar nenhuma, todas serão exportadas):",
        options=tabelas
    )

    if st.button("Gerar Excel"):
        arquivo = exportar_excel(tabelas_selecionadas)

        st.download_button(
            label="⬇️ Baixar arquivo Excel",
            data=arquivo,
            file_name="cbhpm_exportacao.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
