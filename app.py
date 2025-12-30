import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# =====================================================
# CONFIGURAÇÃO SUPABASE (via Secrets)
# =====================================================
DATABASE_URL = st.secrets["SUPABASE_DATABASE_URL"]
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# =====================================================
# FUNÇÃO UTILITÁRIA
# =====================================================
def to_float(valor):
    try:
        if valor is None:
            return 0.0
        if isinstance(valor, str):
            valor = valor.replace(",", ".").strip()
            if valor == "":
                return 0.0
        return float(valor)
    except:
        return 0.0

# =====================================================
# BANCO DE DADOS
# =====================================================
def criar_tabela():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS procedimentos (
                id SERIAL PRIMARY KEY,
                codigo TEXT,
                descricao TEXT,
                porte NUMERIC,
                uco NUMERIC,
                filme NUMERIC,
                versao TEXT,
                UNIQUE (codigo, versao)
            )
        """))

# =====================================================
# IMPORTAÇÃO
# =====================================================
def importar_csvs(arquivos, nome_tabela):
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
            df['versao'] = nome_tabela

            df.columns = ['codigo', 'descricao', 'porte', 'uco', 'filme', 'versao']

            for col in ['porte', 'uco', 'filme']:
                df[col] = df[col].apply(to_float)

            with engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(text("""
                        INSERT INTO procedimentos
                        (codigo, descricao, porte, uco, filme, versao)
                        VALUES (:codigo, :descricao, :porte, :uco, :filme, :versao)
                        ON CONFLICT (codigo, versao) DO NOTHING
                    """), row.to_dict())

        except Exception as e:
            st.error(f"Erro ao importar {arquivo.name}: {e}")

# =====================================================
# CONSULTAS
# =====================================================
def listar_versoes():
    with engine.connect() as conn:
        df = pd.read_sql(
            "SELECT DISTINCT versao FROM procedimentos ORDER BY versao",
            conn
        )
    return df['versao'].tolist()

def buscar_por_codigo(codigo, versao):
    with engine.connect() as conn:
        return pd.read_sql(
            """
            SELECT codigo, descricao, porte, uco, filme
            FROM procedimentos
            WHERE codigo ILIKE %s AND versao = %s
            """,
            conn,
            params=(codigo, versao)
        )

def buscar_por_descricao(descricao, versao):
    with engine.connect() as conn:
        return pd.read_sql(
            """
            SELECT codigo, descricao, porte, uco, filme
            FROM procedimentos
            WHERE descricao ILIKE %s AND versao = %s
            """,
            conn,
            params=(f"%{descricao}%", versao)
        )

# =====================================================
# INTERFACE
# =====================================================
st.set_page_config(page_title="CBHPM – Supabase", layout="wide")
st.title("📊 CBHPM – Banco Permanente (Supabase)")

criar_tabela()

menu = st.sidebar.radio(
    "Menu",
    ["📥 Importar CBHPM", "📋 Consultar", "🧮 Painel de Cálculo"]
)

# =====================================================
# IMPORTAÇÃO
# =====================================================
if menu == "📥 Importar CBHPM":
    st.subheader("Importar tabela CBHPM")

    nome_tabela = st.text_input("Nome da Tabela / Versão")
    arquivos = st.file_uploader(
        "Selecione os CSVs",
        type="csv",
        accept_multiple_files=True
    )

    if st.button("🚀 Importar"):
        if not nome_tabela or not arquivos:
            st.warning("Informe nome da tabela e os arquivos.")
        else:
            importar_csvs(arquivos, nome_tabela)
            st.success("Importação concluída com sucesso!")

# =====================================================
# CONSULTA
# =====================================================
if menu == "📋 Consultar":
    st.subheader("Consulta de Procedimentos")

    versoes = listar_versoes()

    if versoes:
        versao = st.selectbox("Tabela CBHPM", versoes)
        tipo = st.radio("Buscar por", ["Código", "Descrição"])

        termo = st.text_input("Digite o termo")

        if st.button("🔎 Buscar"):
            if tipo == "Código":
                df = buscar_por_codigo(termo, versao)
            else:
                df = buscar_por_descricao(termo, versao)

            if df.empty:
                st.warning("Nenhum resultado encontrado.")
            else:
                st.dataframe(df, use_container_width=True)
    else:
        st.warning("Nenhuma tabela importada.")

# =====================================================
# PAINEL DE CÁLCULO
# =====================================================
if menu == "🧮 Painel de Cálculo":
    st.subheader("Painel de Cálculo CBHPM")

    versoes = listar_versoes()

    if versoes:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            versao = st.selectbox("Tabela CBHPM", versoes)

        with col2:
            codigo = st.text_input("Código")

        with col3:
            valor_filme = st.number_input("Valor Filme (m²)", 0.0, 1000.0, 21.70)

        with col4:
            inflator = st.number_input("Inflator (%)", 0.0, 500.0, 0.0)

        if st.button("🧮 Calcular"):
            df = buscar_por_codigo(codigo, versao)

            if df.empty:
                st.warning("Procedimento não encontrado.")
            else:
                p = df.iloc[0]

                porte = to_float(p['porte'])
                uco = to_float(p['uco'])
                filme = to_float(p['filme'])

                fator = 1 + (inflator / 100)
                porte_corr = porte * fator
                uco_corr = uco * fator

                total = porte_corr + uco_corr + (filme * valor_filme)

                st.info(p['descricao'])

                c1, c2, c3 = st.columns(3)
                c1.metric("Porte corrigido", f"R$ {porte_corr:,.2f}")
                c2.metric("UCO corrigido", f"R$ {uco_corr:,.2f}")
                c3.metric("Filme", f"R$ {(filme * valor_filme):,.2f}")

                st.success(f"💰 Valor Total: R$ {total:,.2f}")
    else:
        st.warning("Nenhuma tabela importada.")
