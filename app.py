import streamlit as st
import pandas as pd
import psycopg2

# =========================
# CONEXÃO SUPABASE
# =========================
def get_connection():
    return psycopg2.connect(
        host=st.secrets["SUPABASE_HOST"],
        database=st.secrets["SUPABASE_DB"],
        user=st.secrets["SUPABASE_USER"],
        password=st.secrets["SUPABASE_PASSWORD"],
        port=st.secrets["SUPABASE_PORT"]
    )

# =========================
# INSERÇÃO DE DADOS
# =========================
def inserir_dados(df, tabela_nome):
    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            insert into cbhpm (codigo, descricao, porte, uco, tabela_nome)
            values (%s, %s, %s, %s, %s)
        """, (
            str(row["codigo"]),
            row["descricao"],
            float(row["porte"]),
            float(row["uco"]),
            tabela_nome
        ))

    conn.commit()
    cur.close()
    conn.close()

# =========================
# CONSULTAS
# =========================
def consultar_procedimentos(tabela_nome, descricao=""):
    conn = get_connection()
    query = """
        select * from cbhpm
        where tabela_nome = %s
        and descricao ilike %s
        order by descricao
    """
    df = pd.read_sql(query, conn, params=(tabela_nome, f"%{descricao}%"))
    conn.close()
    return df

def listar_tabelas():
    conn = get_connection()
    df = pd.read_sql(
        "select distinct tabela_nome from cbhpm order by tabela_nome",
        conn
    )
    conn.close()
    return df["tabela_nome"].tolist()

# =========================
# INTERFACE
# =========================
st.set_page_config("CBHPM Permanente", layout="wide")

menu = st.sidebar.selectbox(
    "Menu",
    ["📥 Importar Tabela", "🔎 Consulta", "🧮 Painel de Cálculo"]
)

# =========================
# IMPORTAÇÃO
# =========================
if menu == "📥 Importar Tabela":
    st.header("Importar CBHPM (1 vez apenas)")

    tabela_nome = st.text_input("Nome da tabela (ex: CBHPM 2024)")
    arquivo = st.file_uploader("CSV da CBHPM", type=["csv"])

    if arquivo and tabela_nome:
        df = pd.read_csv(arquivo)
        st.dataframe(df.head())

        if st.button("Salvar no banco permanente"):
            inserir_dados(df, tabela_nome)
            st.success("Tabela salva com sucesso no Supabase")

# =========================
# CONSULTA
# =========================
elif menu == "🔎 Consulta":
    st.header("Consulta por descrição")

    tabelas = listar_tabelas()
    tabela = st.selectbox("Tabela", tabelas)

    descricao = st.text_input("Buscar por descrição")

    if tabela:
        df = consultar_procedimentos(tabela, descricao)
        st.dataframe(df)

# =========================
# CÁLCULO
# =========================
elif menu == "🧮 Painel de Cálculo":
    st.header("Cálculo CBHPM")

    tabelas = listar_tabelas()
    tabela = st.selectbox("Tabela", tabelas)

    if tabela:
        df = consultar_procedimentos(tabela)
        procedimento = st.selectbox(
            "Procedimento",
            df["descricao"]
        )

        proc = df[df["descricao"] == procedimento].iloc[0]

        valor_uco = st.number_input("Valor da UCO (R$)", value=100.0)
        inflator = st.number_input("Inflator (%)", value=0.0)

        porte = float(proc["porte"])
        uco = float(proc["uco"])

        valor_base = porte * uco * valor_uco
        fator = 1 + (inflator / 100)
        valor_final = valor_base * fator

        st.metric("Valor Base", f"R$ {valor_base:,.2f}")
        st.metric("Valor Final (com inflator)", f"R$ {valor_final:,.2f}")
