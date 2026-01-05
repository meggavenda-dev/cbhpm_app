import streamlit as st
import pandas as pd
import sqlite3

# =====================================================
# CONFIGURAÇÃO BANCO LOCAL (SQLite)
# =====================================================
DB_NAME = "cbhpm_local.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

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
# CRIAÇÃO DA TABELA
# =====================================================
def criar_tabela():
    conn = get_connection()
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
# IMPORTAÇÃO CSV
# =====================================================
def importar_csvs(arquivos, versao):
    conn = get_connection()
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

            # 🔎 Normaliza nomes das colunas
            df.columns = [c.strip() for c in df.columns]

            # Mapeamento flexível
            mapa = {
                'codigo': ['Código', 'Codigo', 'CODIGO'],
                'descricao': ['Descrição', 'Descricao'],
                'porte': ['Porte', 'Porte Cirúrgico', 'Porte Anestésico'],
                'uco': ['UCO', 'UCO (CH)', 'CH', 'UCO_CBPM'],
                'filme': ['Filme', 'Filme Radiológico', 'Filme Rx']
            }

            dados = {}

            for campo, possiveis in mapa.items():
                col = next((c for c in possiveis if c in df.columns), None)
                if col:
                    dados[campo] = df[col]
                else:
                    dados[campo] = 0.0  # se não existir, zera

            df_final = pd.DataFrame(dados)
            df_final['versao'] = versao

            for col in ['porte', 'uco', 'filme']:
                df_final[col] = df_final[col].apply(to_float)

            for _, row in df_final.iterrows():
                cursor.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, tuple(row))

        except Exception as e:
            st.error(f"Erro ao importar {arquivo.name}: {e}")

    conn.commit()
    conn.close()


# =====================================================
# CONSULTAS
# =====================================================
def listar_versoes():
    conn = get_connection()
    df = pd.read_sql(
        "SELECT DISTINCT versao FROM procedimentos ORDER BY versao",
        conn
    )
    conn.close()
    return df['versao'].tolist()

def buscar_por_codigo(codigo, versao):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE codigo LIKE ? AND versao = ?
        """,
        conn,
        params=(f"%{codigo}%", versao)
    )
    conn.close()
    return df

def buscar_por_descricao(descricao, versao):
    conn = get_connection()
    df = pd.read_sql(
        """
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE descricao LIKE ? AND versao = ?
        """,
        conn,
        params=(f"%{descricao}%", versao)
    )
    conn.close()
    return df

# =====================================================
# INTERFACE
# =====================================================
st.set_page_config(page_title="CBHPM – Banco Local", layout="wide")
st.title("📊 CBHPM – Banco Local (SQLite)")

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

    versao = st.text_input("Nome da Tabela / Versão")
    arquivos = st.file_uploader(
        "Selecione os CSVs",
        type="csv",
        accept_multiple_files=True
    )

    if st.button("🚀 Importar"):
        if not versao or not arquivos:
            st.warning("Informe o nome da versão e selecione os arquivos.")
        else:
            importar_csvs(arquivos, versao)
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
