import streamlit as st
import pandas as pd
import sqlite3

DB_NAME = "cbhpm_database.db"

# =====================================================
# UTILITÁRIOS
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


def ler_csv_seguro(arquivo):
    """
    Leitura robusta para CSVs problemáticos (Excel antigo / CBHPM)
    """
    try:
        return pd.read_csv(
            arquivo,
            sep=";",
            encoding="latin-1",
            engine="python",
            on_bad_lines="skip"
        )
    except Exception:
        return pd.read_csv(
            arquivo,
            sep=";",
            encoding="cp1252",
            engine="python",
            on_bad_lines="skip"
        )

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

# =====================================================
# IMPORTAÇÃO
# =====================================================
def importar_csvs(arquivos, nome_tabela):
    conn = get_conn()
    cursor = conn.cursor()

    for arquivo in arquivos:
        try:
            df = ler_csv_seguro(arquivo)

            df = df[['Código', 'Descrição', 'Porte', 'UCO', 'Filme']]
            df['versao'] = nome_tabela

            df.columns = [
                'codigo',
                'descricao',
                'porte',
                'uco',
                'filme',
                'versao'
            ]

            for col in ['porte', 'uco', 'filme']:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", ".", regex=False)
                    .replace("nan", "0")
                )

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row['codigo'],
                    row['descricao'],
                    to_float(row['porte']),
                    to_float(row['uco']),
                    to_float(row['filme']),
                    row['versao']
                ))

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


def buscar_procedimento(codigo, descricao, versao):
    conn = get_conn()

    query = """
        SELECT codigo, descricao, porte, uco, filme
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
# INTERFACE
# =====================================================
st.set_page_config(page_title="CBHPM – Painel", layout="wide")
st.title("📊 CBHPM – Banco de Dados e Painel de Cálculo")

criar_tabela()

menu = st.sidebar.radio(
    "Menu",
    ["📥 Importar CBHPM", "📋 Consultar", "🧮 Painel de Cálculo"]
)

# =====================================================
# ABA IMPORTAÇÃO
# =====================================================
if menu == "📥 Importar CBHPM":
    st.subheader("Importar tabela CBHPM")

    nome_tabela = st.text_input(
        "Nome da Tabela / Versão",
        placeholder="Ex: CBHPM 2022 Oficial"
    )

    arquivos = st.file_uploader(
        "Selecione os arquivos CSV",
        type="csv",
        accept_multiple_files=True
    )

    if st.button("🚀 Importar dados"):
        if not nome_tabela:
            st.warning("Informe um nome para a tabela.")
        elif not arquivos:
            st.warning("Selecione ao menos um CSV.")
        else:
            importar_csvs(arquivos, nome_tabela)
            st.success("Importação concluída com sucesso!")

# =====================================================
# ABA CONSULTA
# =====================================================
if menu == "📋 Consultar":
    st.subheader("Consulta de Procedimentos")

    versoes = listar_versoes()

    if not versoes:
        st.warning("Nenhuma tabela importada.")
    else:
        col1, col2, col3 = st.columns(3)

        with col1:
            versao = st.selectbox("Tabela CBHPM", versoes)

        with col2:
            codigo = st.text_input("Código (opcional)")

        with col3:
            descricao = st.text_input("Descrição (opcional)")

        if st.button("🔎 Pesquisar"):
            df = buscar_procedimento(codigo, descricao, versao)

            if df.empty:
                st.warning("Nenhum procedimento encontrado.")
            else:
                st.dataframe(df, use_container_width=True)

# =====================================================
# PAINEL DE CÁLCULO
# =====================================================
if menu == "🧮 Painel de Cálculo":
    st.subheader("Painel de Cálculo CBHPM")

    versoes = listar_versoes()

    if not versoes:
        st.warning("Nenhuma tabela importada.")
    else:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            versao = st.selectbox("Tabela CBHPM", versoes)

        with col2:
            codigo = st.text_input("Código TUSS")

        with col3:
            valor_filme = st.number_input(
                "Valor do Filme (m²)",
                min_value=0.0,
                value=21.70,
                step=0.01
            )

        with col4:
            inflator = st.number_input(
                "Inflator (%)",
                min_value=0.0,
                value=0.0,
                step=0.5
            )

        if st.button("🧮 Calcular"):
            df = buscar_procedimento(codigo, None, versao)

            if df.empty:
                st.warning("Procedimento não encontrado.")
            else:
                proc = df.iloc[0]

                porte = to_float(proc['porte'])
                uco = to_float(proc['uco'])
                qtd_filme = to_float(proc['filme'])

                fator = 1 + (inflator / 100)

                porte_corrigido = porte * fator
                uco_corrigido = uco * fator
                total_filme = qtd_filme * valor_filme

                total = porte_corrigido + uco_corrigido + total_filme

                st.info(f"**Descrição:** {proc['descricao']}")

                c1, c2, c3 = st.columns(3)
                c1.metric("Porte (corrigido)", f"R$ {porte_corrigido:,.2f}")
                c2.metric("UCO (corrigida)", f"R$ {uco_corrigido:,.2f}")
                c3.metric("Filme", f"R$ {total_filme:,.2f}")

                st.success(f"### 💰 Valor Total: R$ {total:,.2f}")
