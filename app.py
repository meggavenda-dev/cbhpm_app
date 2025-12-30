import streamlit as st
import sqlite3
import pandas as pd

st.set_page_config(page_title="Painel CBHPM", layout="centered")
st.title("📊 Painel de Consulta CBHPM")
st.markdown("---")

col1, col2 = st.columns(2)
with col1:
    versao_selecionada = st.selectbox(
        "Selecione a tabela CBHPM:",
        ["CBHPM 2022", "CBHPM 2020", "CBHPM 2018", "CBHPM 2016", 
         "CBHPM 2014", "CBHPM 2012", "CBHPM 2010", "CBHPM 5 (2009)", 
         "CBHPM 5 (2008)", "CBHPM 4", "CBHPM 3"]
    )
with col2:
    valor_filme_unit = st.number_input("Valor do Filme (m²):", min_value=0.0, value=21.70, step=0.01)

codigo_tuss = st.text_input("Informe o código TUSS:", placeholder="Ex: 40807061")

def buscar_dados(codigo, versao):
    conn = sqlite3.connect('cbhpm_database.db')
    df = pd.read_sql_query(
        "SELECT descricao, porte, uco, filme FROM procedimentos WHERE codigo=? AND versao=?",
        conn, params=(codigo, versao)
    )
    conn.close()
    return df

if codigo_tuss:
    resultado = buscar_dados(codigo_tuss.strip(), versao_selecionada)
    if not resultado.empty:
        desc = resultado.iloc[0]['descricao']
        porte = float(resultado.iloc[0]['porte'])
        uco = float(resultado.iloc[0]['uco'])
        qtd_filme = float(resultado.iloc[0]['filme'])
        total_filme = qtd_filme * valor_filme_unit
        valor_total = porte + uco + total_filme

        st.info(f"**Descrição do Procedimento:**\n\n{desc}")
        m1, m2, m3 = st.columns(3)
        m1.metric("Valor do Porte", f"R$ {porte:,.2f}")
        m2.metric("Valor da UCO", f"R$ {uco:,.2f}")
        m3.metric("Valor do Filme", f"R$ {total_filme:,.2f}")
        st.success(f"### Valor Total: R$ {valor_total:,.2f}")
    else:
        st.warning("Procedimento não encontrado para este código nesta versão.")

st.markdown("---")
st.caption("Base de dados carregada via SQLite conforme estrutura original da planilha.")
