import streamlit as st
import sqlite3
import pandas as pd

st.set_page_config(page_title="Painel CBHPM", layout="wide")

# ---------- FUNÇÕES ----------
@st.cache_data
def carregar_banco():
    conn = sqlite3.connect("cbhpm_database.db")
    df = pd.read_sql("SELECT * FROM procedimentos", conn)
    conn.close()
    return df

def buscar_procedimento(codigo, versao):
    conn = sqlite3.connect("cbhpm_database.db")
    df = pd.read_sql(
        "SELECT descricao, porte, uco, filme FROM procedimentos WHERE codigo=? AND versao=?",
        conn,
        params=(codigo, versao)
    )
    conn.close()
    return df


# ---------- ABAS ----------
aba1, aba2 = st.tabs(["📊 Painel CBHPM", "🗄️ Banco de Dados"])

# ==============================
# ABA 1 – PAINEL CBHPM
# ==============================
with aba1:
    st.title("📊 Painel de Consulta CBHPM")

    col1, col2 = st.columns(2)

    with col1:
        versao = st.selectbox(
            "Selecione a tabela CBHPM:",
            [
                "CBHPM 2022", "CBHPM 2020", "CBHPM 2018", "CBHPM 2016",
                "CBHPM 2014", "CBHPM 2012", "CBHPM 2010",
                "CBHPM 5 (2009)", "CBHPM 5 (2008)", "CBHPM 4", "CBHPM 3"
            ]
        )

    with col2:
        valor_filme = st.number_input(
            "Valor do Filme (m²):",
            min_value=0.0,
            value=21.70,
            step=0.01
        )

    codigo = st.text_input("Código TUSS")

    if codigo:
        resultado = buscar_procedimento(codigo.strip(), versao)

        if not resultado.empty:
            desc = resultado.iloc[0]["descricao"]
            porte = float(resultado.iloc[0]["porte"])
            uco = float(resultado.iloc[0]["uco"])
            qtd_filme = float(resultado.iloc[0]["filme"])

            total_filme = qtd_filme * valor_filme
            total = porte + uco + total_filme

            st.info(f"**Descrição:** {desc}")

            m1, m2, m3 = st.columns(3)
            m1.metric("Porte", f"R$ {porte:,.2f}")
            m2.metric("UCO", f"R$ {uco:,.2f}")
            m3.metric("Filme", f"R$ {total_filme:,.2f}")

            st.success(f"### Valor Total: R$ {total:,.2f}")
        else:
            st.warning("Procedimento não encontrado.")

# ==============================
# ABA 2 – VISUALIZAR BANCO
# ==============================
with aba2:
    st.title("🗄️ Banco de Dados – Procedimentos CBHPM")

    df = carregar_banco()

    colf1, colf2 = st.columns(2)

    with colf1:
        versao_filtro = st.selectbox(
            "Filtrar por versão:",
            ["Todas"] + sorted(df["versao"].unique().tolist())
        )

    with colf2:
        codigo_filtro = st.text_input("Filtrar por código (opcional)")

    if versao_filtro != "Todas":
        df = df[df["versao"] == versao_filtro]

    if codigo_filtro:
        df = df[df["codigo"].str.contains(codigo_filtro, na=False)]

    st.write(f"🔢 Total de registros: {len(df)}")

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True
    )
