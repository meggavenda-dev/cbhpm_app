import os
import base64
import hashlib
import time
import sqlite3
from io import BytesIO
from contextlib import contextmanager
from datetime import datetime

import pandas as pd
import requests
import altair as alt
import streamlit as st

# =====================================================
# CONFIGURAÇÕES E ESTADO DA SESSÃO
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

# Estados iniciais
if 'comparacao_realizada' not in st.session_state:
    st.session_state.comparacao_realizada = False
if 'aba_ativa' not in st.session_state:
    st.session_state.aba_ativa = "📥 Importar"

# =====================================================
# CONEXÃO E BANCO DE DADOS
# =====================================================
@st.cache_resource
def get_connection():
    # check_same_thread=False é essencial para Streamlit (multithread)
    return sqlite3.connect(DB_NAME, check_same_thread=False, timeout=30)

@contextmanager
def gerenciar_db():
    con = get_connection()
    try:
        yield con
        con.commit()
    except Exception as e:
        con.rollback()
        raise e

def criar_tabelas():
    with gerenciar_db() as con:
        cur = con.cursor()
        cur.execute("""
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_proc_cod ON procedimentos (codigo)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_proc_ver ON procedimentos (versao)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS arquivos_importados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                versao TEXT,
                data TEXT
            )
        """)

# =====================================================
# UTILITÁRIOS
# =====================================================
def to_float(v):
    if pd.isna(v) or v == "": return 0.0
    if isinstance(v, str):
        v = v.replace(".", "").replace(",", ".").strip()
    try: return float(v)
    except: return 0.0

def gerar_hash_arquivo(uploaded_file):
    uploaded_file.seek(0)
    h = hashlib.sha256(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h

def extrair_valor(row, df, col_opts):
    for c in col_opts:
        if c in df.columns: return to_float(row[c])
    return 0.0

# =====================================================
# GITHUB – PERSISTÊNCIA
# =====================================================
def baixar_banco():
    if os.path.exists(DB_NAME): return
    try:
        url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        headers = {"Authorization": f"token {st.secrets['GITHUB_TOKEN']}", "Accept": "application/vnd.github.v3+json"}
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            content = r.json()["content"]
            with open(DB_NAME, "wb") as f:
                f.write(base64.b64decode(content))
        else: open(DB_NAME, "wb").close()
    except: pass

def salvar_banco_github(msg):
    try:
        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        api_url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        headers = {"Authorization": f"token {st.secrets['GITHUB_TOKEN']}", "Accept": "application/vnd.github.v3+json"}
        r = requests.get(api_url, headers=headers)
        sha = r.json().get("sha") if r.status_code == 200 else None
        payload = {"message": msg, "content": content, "branch": st.secrets["GITHUB_BRANCH"]}
        if sha: payload["sha"] = sha
        requests.put(api_url, headers=headers, json=payload)
    except: st.warning("Erro na sincronização com GitHub.")

# =====================================================
# LÓGICA DE NEGÓCIO
# =====================================================
def importar(arquivos, versao):
    if not versao:
        st.error("Informe a Versão CBHPM.")
        return False
    mapa = {"codigo": ["Código", "Codigo"], "descricao": ["Descrição", "Descricao"], 
            "porte": ["Porte"], "uco": ["UCO", "CH"], "filme": ["Filme"]}
    
    arquivos_processados = 0
    with gerenciar_db() as con:
        cur = con.cursor()
        for arq in arquivos:
            h = gerar_hash_arquivo(arq)
            cur.execute("SELECT 1 FROM arquivos_importados WHERE hash=?", (h,))
            if cur.fetchone():
                st.warning(f"O arquivo '{arq.name}' já foi importado anteriormente.")
                continue

            try:
                if arq.name.lower().endswith(".csv"):
                    try: df = pd.read_csv(arq, sep=";", encoding="utf-8")
                    except: 
                        arq.seek(0)
                        df = pd.read_csv(arq, sep=";", encoding="latin-1")
                else: df = pd.read_excel(arq)
            except Exception as e:
                st.error(f"Erro ao ler {arq.name}: {e}")
                continue

            df.columns = [c.strip() for c in df.columns]
            dados_lista = []
            for _, row in df.iterrows():
                d = {campo: extrair_valor(row, df, cols) for campo, cols in mapa.items()}
                cod = str(row[next((c for c in mapa["codigo"] if c in df.columns))])
                desc = str(row[next((c for c in mapa["descricao"] if c in df.columns))])
                dados_lista.append((cod, desc, d["porte"], d["uco"], d["filme"], versao))

            cur.executemany("INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao) VALUES (?, ?, ?, ?, ?, ?)", dados_lista)
            cur.execute("INSERT OR IGNORE INTO arquivos_importados (hash, versao, data) VALUES (?, ?, ?)", (h, versao, datetime.now().isoformat()))
            arquivos_processados += 1

    if arquivos_processados > 0:
        salvar_banco_github(f"Importação {versao}")
        return True
    return False

@st.cache_data
def versoes():
    with get_connection() as con:
        try: return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except: return []

def buscar_dados(termo, versao, tipo):
    campo = "codigo" if tipo == "Código" else "descricao"
    with get_connection() as con:
        return pd.read_sql(f"SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE {campo} LIKE ? AND versao = ?", 
                           con, params=(f"%{termo}%", versao))

# =====================================================
# INTERFACE STREAMLIT
# =====================================================
baixar_banco()
criar_tabelas()

st.set_page_config(page_title="CBHPM Gestão Inteligente", layout="wide")
st.title("⚖️ CBHPM • Auditoria e Gestão")

lista_v = versoes()
v_selecionada = st.sidebar.selectbox("Tabela Ativa", lista_v, key="v_global") if lista_v else None

abas = st.tabs(["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar"])

# --- 1. IMPORTAR ---
# --- 1. IMPORTAR (VERSÃO FINAL - VISUAL LIMPO) ---
with abas[0]:
    st.subheader("Carregar Novos Dados")

    # Criamos um espaço vazio que conterá o formulário ou as mensagens de sucesso
    area_importacao = st.empty()

    # Se a importação ainda não começou ou não foi processada neste ciclo
    with area_importacao.container():
        with st.form("form_importacao", clear_on_submit=True):
            v_imp = st.text_input("Nome da Versão (ex: CBHPM 2024)")
            arqs = st.file_uploader("Upload arquivos (CSV ou Excel)", accept_multiple_files=True)
            submitted = st.form_submit_button("🚀 Iniciar Importação Agora")
            
            if submitted:
                if not v_imp:
                    st.error("Por favor, dê um nome para esta versão antes de importar.")
                elif not arqs:
                    st.warning("Nenhum arquivo selecionado.")
                else:
                    # O "Pulo do Gato": Limpamos o formulário da tela IMEDIATAMENTE
                    area_importacao.empty()
                    
                    # Agora trabalhamos em um container limpo
                    with st.container():
                        st.info(f"🔄 Iniciando processamento da versão: **{v_imp}**")
                        
                        # A barra de progresso (dentro da função importar) aparecerá aqui
                        if importar(arqs, v_imp):
                            # Notificação toast rápida
                            st.toast(f"Tabela {v_imp} processada!", icon="✅")
                            
                            # Mensagem final de sucesso
                            st.success("✅ Importação concluída com sucesso! Atualizando sistema...")
                            
                            # Atualização de estados
                            st.cache_data.clear()
                            st.session_state.lista_versoes = versoes()
                            
                            # Aguarda para leitura
                            time.sleep(2)
                            
                            # Reinicia o app - ele voltará com a sidebar atualizada e o form vazio
                            st.rerun()
                        else:
                            st.error("Erro durante a importação. Verifique os arquivos.")
                            # Botão para voltar ao form se houver erro
                            if st.button("Voltar"):
                                st.rerun()

# --- 2. CONSULTAR ---
with abas[1]:
    if v_selecionada:
        st.info(f"Pesquisando na Versão: {v_selecionada}")
        c1, c2 = st.columns([1, 3])
        tipo = c1.radio("Busca por", ["Código", "Descrição"], horizontal=True)
        termo = c2.text_input("Digite o termo de busca...")
        if termo:
            res = buscar_dados(termo, v_selecionada, tipo)
            st.dataframe(res, use_container_width=True, hide_index=True)

# --- 3. CALCULAR ---
with abas[2]:
    if v_selecionada:
        cod_calc = st.text_input("Código do Procedimento", key="in_calc")
        col1, col2, col3 = st.columns(3)
        uco_v = col1.number_input("Valor UCO (R$)", 1.0, step=0.1)
        filme_v = col2.number_input("Valor Filme (R$)", 21.70, step=0.1)
        infla = col3.number_input("Ajuste Adicional (%)", 0.0)
        
        if st.button("Calcular Honorários"):
            res = buscar_dados(cod_calc, v_selecionada, "Código")
            if not res.empty:
                p = res.iloc[0]
                f = 1 + (infla/100)
                total = (p['porte']*f) + (p['uco']*uco_v*f) + (p['filme']*filme_v*f)
                st.metric(f"Total: {p['descricao']}", f"R$ {total:,.2f}")
            else: st.error("Código não encontrado.")

# --- 4. COMPARAR ---
with abas[3]:
    if len(lista_v) >= 2:
        col1, col2 = st.columns(2)
        v1 = col1.selectbox("Versão Anterior", lista_v, key="v1")
        v2 = col2.selectbox("Versão Atual", lista_v, key="v2")
        
        if st.button("Analisar Reajustes"):
            st.session_state.comparacao_realizada = True
        
        if st.session_state.comparacao_realizada:
            df1 = buscar_dados("", v1, "Código")
            df2 = buscar_dados("", v2, "Código").rename(columns={"porte":"porte_2", "uco":"uco_2", "filme":"filme_2", "descricao":"desc_2"})
            comp = df1.merge(df2, on="codigo")
            
            if not comp.empty:
                comp['var_porte'] = ((comp['porte_2'] - comp['porte']) / comp['porte'].replace(0,1)) * 100
                
                # Resumo das Métricas
                m1, m2, m3 = st.columns(3)
                m1.metric("Itens Comuns", len(comp))
                m2.metric("Variação Média Porte", f"{comp['var_porte'].mean():.2f}%")
                m3.metric("Itens com Aumento", len(comp[comp['var_porte'] > 0]))

                # Gráfico
                resumo = comp.groupby(comp['codigo'].str[:2])['var_porte'].mean().reset_index()
                chart = alt.Chart(resumo).mark_bar().encode(
                    x=alt.X('codigo:N', title="Grupo (Capítulo)"),
                    y=alt.Y('var_porte:Q', title="Variação %"),
                    color=alt.condition(alt.datum.var_porte > 0, alt.value('steelblue'), alt.value('orange'))
                ).properties(height=350)
                st.altair_chart(chart, use_container_width=True)

                st.dataframe(comp[['codigo', 'descricao', 'porte', 'porte_2', 'var_porte']], 
                             use_container_width=True, hide_index=True,
                             column_config={"var_porte": st.column_config.NumberColumn("Variação %", format="%.2f%%")})
    else: st.warning("Necessário ao menos 2 versões para comparar.")

# --- 5. EXPORTAR ---
with abas[4]:
    if lista_v:
        if st.button("📦 Gerar Backup Completo (Excel)"):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                with get_connection() as con:
                    pd.read_sql("SELECT * FROM procedimentos", con).to_excel(writer, index=False)
            st.download_button("📥 Baixar Arquivo", output.getvalue(), "cbhpm_completa.xlsx")

# --- 6. GERENCIAR ---
with abas[5]:
    if lista_v:
        v_del = st.selectbox("Versão para Exclusão", lista_v)
        confirmar = st.checkbox("Confirmo a exclusão definitiva desta versão e sincronização com GitHub.")
        if st.button("🗑️ Deletar Versão", type="primary"):
            if confirmar:
                with gerenciar_db() as con:
                    con.execute("DELETE FROM procedimentos WHERE versao=?", (v_del,))
                    con.execute("DELETE FROM arquivos_importados WHERE versao=?", (v_del,))
                salvar_banco_github(f"Remoção da versão {v_del}")
                st.cache_data.clear()
                st.success("Versão removida!")
                time.sleep(1)
                st.rerun()
