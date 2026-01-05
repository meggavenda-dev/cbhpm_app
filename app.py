import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

# =====================================================
# CONFIG BANCO LOCAL
# =====================================================
DB_NAME = "cbhpm_local.db"

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

# =====================================================
# UTIL
# =====================================================
def to_float(valor):
    try:
        if pd.isna(valor):
            return 0.0
        if isinstance(valor, str):
            valor = valor.replace(",", ".").strip()
            if valor == "":
                return 0.0
        return float(valor)
    except:
        return 0.0

# =====================================================
# TABELAS
# =====================================================
def criar_tabelas():
    conn = get_connection()
    c = conn.cursor()

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

    conn.commit()
    conn.close()

# =====================================================
# LEITURA FLEXÍVEL
# =====================================================
def ler_arquivo(arquivo):
    if arquivo.name.lower().endswith(".csv"):
        return pd.read_csv(
            arquivo,
            encoding="latin-1",
            sep=";",
            engine="python",
            on_bad_lines="skip"
        )
    else:
        return pd.read_excel(arquivo)

# =====================================================
# IMPORTAÇÃO INTELIGENTE
# =====================================================
def importar_arquivos(arquivos, versao):
    conn = get_connection()
    c = conn.cursor()

    mapa_colunas = {
        'codigo': ['Código', 'Codigo', 'CODIGO'],
        'descricao': ['Descrição', 'Descricao'],
        'porte': ['Porte', 'Porte Cirúrgico', 'Porte Anestésico'],
        'uco': ['UCO', 'UCO (CH)', 'CH', 'UCO_CBPM'],
        'filme': ['Filme', 'Filme Radiológico', 'Filme Rx']
    }

    for arquivo in arquivos:
        try:
            df = ler_arquivo(arquivo)
            df.columns = [col.strip() for col in df.columns]

            encontrados = {}
            ausentes = []

            for campo, possibilidades in mapa_colunas.items():
                col = next((c for c in possibilidades if c in df.columns), None)
                if col:
                    encontrados[campo] = col
                else:
                    ausentes.append(campo)

            # LOG DE COLUNAS AUSENTES
            if ausentes:
                c.execute("""
                    INSERT INTO log_importacao (versao, arquivo, problema, data)
                    VALUES (?, ?, ?, ?)
                """, (
                    versao,
                    arquivo.name,
                    f"Colunas ausentes: {', '.join(ausentes)}",
                    datetime.now().isoformat()
                ))

            # MONTA DATAFRAME FINAL
            dados = {}
            for campo in mapa_colunas.keys():
                if campo in encontrados:
                    dados[campo] = df[encontrados[campo]]
                else:
                    dados[campo] = 0.0

            df_final = pd.DataFrame(dados)
            df_final['versao'] = versao

            for col in ['porte', 'uco', 'filme']:
                df_final[col] = df_final[col].apply(to_float)

            # INSERÇÃO
            for _, row in df_final.iterrows():
                c.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, tuple(row))

            # VISUAL
            st.success(f"✔ {arquivo.name} importado")
            st.caption(f"Colunas detectadas: {encontrados}")

        except Exception as e:
            c.execute("""
                INSERT INTO log_importacao (versao, arquivo, problema, data)
                VALUES (?, ?, ?, ?)
            """, (
                versao,
                arquivo.name,
                str(e),
                datetime.now().isoformat()
            ))
            st.error(f"Erro em {arquivo.name}: {e}")

    conn.commit()
    conn.close()

# =====================================================
# CONSULTAS
# =====================================================
def listar_versoes():
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", conn)
    conn.close()
    return df['versao'].tolist()

def buscar_codigo(codigo, versao):
    conn = get_connection()
    df = pd.read_sql("""
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE codigo LIKE ? AND versao = ?
    """, conn, params=(f"%{codigo}%", versao))
    conn.close()
    return df

def buscar_descricao(desc, versao):
    conn = get_connection()
    df = pd.read_sql("""
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE descricao LIKE ? AND versao = ?
    """, conn, params=(f"%{desc}%", versao))
    conn.close()
    return df

def consultar_logs():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM log_importacao ORDER BY data DESC", conn)
    conn.close()
    return df

# =====================================================
# INTERFACE
# =====================================================
st.set_page_config("CBHPM – Banco Local Profissional", layout="wide")
st.title("📊 CBHPM – Banco Local Profissional")

criar_tabelas()

menu = st.sidebar.radio(
    "Menu",
    ["📥 Importar", "📋 Consultar", "🧮 Calcular", "🧾 Log de Importação"]
)

# =====================================================
# IMPORTAR
# =====================================================
if menu == "📥 Importar":
    st.subheader("Importação Inteligente CBHPM")
    versao = st.text_input("Versão / Ano")
    arquivos = st.file_uploader(
        "CSV ou Excel",
        type=["csv", "xlsx"],
        accept_multiple_files=True
    )

    if st.button("🚀 Importar"):
        if not versao or not arquivos:
            st.warning("Informe a versão e os arquivos.")
        else:
            importar_arquivos(arquivos, versao)

# =====================================================
# CONSULTAR
# =====================================================
if menu == "📋 Consultar":
    versoes = listar_versoes()
    if versoes:
        versao = st.selectbox("Versão", versoes)
        tipo = st.radio("Buscar por", ["Código", "Descrição"])
        termo = st.text_input("Termo")

        if st.button("🔎 Buscar"):
            df = buscar_codigo(termo, versao) if tipo == "Código" else buscar_descricao(termo, versao)
            st.dataframe(df, use_container_width=True)
    else:
        st.warning("Nenhuma tabela importada.")

# =====================================================
# CALCULAR
# =====================================================
if menu == "🧮 Calcular":
    versoes = listar_versoes()
    if versoes:
        versao = st.selectbox("Versão", versoes)
        codigo = st.text_input("Código")
        valor_filme = st.number_input("Valor Filme", 0.0, 1000.0, 21.70)
        inflator = st.number_input("Inflator (%)", 0.0, 500.0, 0.0)

        if st.button("Calcular"):
            df = buscar_codigo(codigo, versao)
            if df.empty:
                st.warning("Não encontrado")
            else:
                p = df.iloc[0]
                fator = 1 + inflator / 100
                total = (p['porte'] + p['uco']) * fator + p['filme'] * valor_filme
                st.success(f"💰 Valor Total: R$ {total:,.2f}")
                st.caption(p['descricao'])

# =====================================================
# LOG
# =====================================================
if menu == "🧾 Log de Importação":
    st.subheader("Inconsistências Detectadas")
    df_log = consultar_logs()
    st.dataframe(df_log, use_container_width=True)
