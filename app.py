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

# --- INICIALIZAÇÃO DE ESTADO ---
if 'lista_versoes' not in st.session_state:
    # Tenta carregar as versões do banco ou inicia vazio
    try:
        st.session_state.lista_versoes = carregar_versoes_db()
    except:
        st.session_state.lista_versoes = []

if 'comparacao_realizada' not in st.session_state:
    st.session_state.comparacao_realizada = False

if 'aba_ativa' not in st.session_state:
    st.session_state.aba_ativa = 0

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

# Criar as abas com uma chave (key) para persistência

abas = st.tabs(["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar"])

# --- 1. IMPORTAR ---
# --- 1. IMPORTAR (VERSÃO FINAL - VISUAL LIMPO) ---
# --- 1. IMPORTAR (VERSÃO CORRIGIDA - SEM TELA BRANCA) ---
# --- 1. IMPORTAR (RESOLVIDO: SEM NAMEERROR E SEM TELA BRANCA) ---
with abas[0]:
    st.subheader("Carregar Novos Dados")

    # Inicializa variáveis de controle no estado da sessão
    if "processando" not in st.session_state:
        st.session_state.processando = False
    if "temp_v_imp" not in st.session_state:
        st.session_state.temp_v_imp = ""
    if "temp_arqs" not in st.session_state:
        st.session_state.temp_arqs = None

    # Espaço dinâmico (Placeholder)
    area_dinamica = st.empty()

    if not st.session_state.processando:
        # EXIBE O FORMULÁRIO
        with area_dinamica.container():
            with st.form("form_importacao", clear_on_submit=True):
                v_imp_input = st.text_input("Nome da Versão (ex: CBHPM 2024)")
                arqs_input = st.file_uploader("Upload arquivos (CSV ou Excel)", accept_multiple_files=True)
                submitted = st.form_submit_button("🚀 Iniciar Importação Agora")
                
                if submitted:
                    if not v_imp_input or not arqs_input:
                        st.error("Preencha o nome da versão e selecione os arquivos.")
                    else:
                        # SALVA NO ESTADO DA SESSÃO PARA O PRÓXIMO CICLO
                        st.session_state.temp_v_imp = v_imp_input
                        st.session_state.temp_arqs = arqs_input
                        st.session_state.processando = True
                        st.rerun() # Reinicia para trocar a tela
    else:
        # EXIBE O STATUS DE PROCESSAMENTO (O formulário sumiu)
        with area_dinamica.container():
            st.info(f"⚙️ Processando: **{st.session_state.temp_v_imp}**")
            
            # Chamamos a função usando os dados salvos no session_state
            if importar(st.session_state.temp_arqs, st.session_state.temp_v_imp):
                st.toast("Dados processados com sucesso!", icon="✅")
                st.success("✅ Importação concluída! O sistema será atualizado.")
                
                # Limpa o cache e as variáveis temporárias
                st.cache_data.clear()
                st.session_state.lista_versoes = versoes()
                st.session_state.processando = False
                st.session_state.temp_arqs = None
                
                time.sleep(2)
                st.rerun()
            else:
                st.error("Erro crítico na importação.")
                if st.button("Tentar Novamente"):
                    st.session_state.processando = False
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
# Guarda a aba ativa
st.session_state.aba_ativa = 2  # índice da aba de cálculo

with abas[2]:
    if v_selecionada:
        st.subheader("🧮 Calculadora de Honorários CBHPM")
        
        st.markdown("""
            <style>
            [data-testid="stMetricValue"] { font-size: 1.8rem; color: #007bff; }
            .res-card { 
                padding: 20px; 
                border-radius: 10px; 
                background-color: #f8f9fa; 
                border-left: 5px solid #007bff;
                margin-bottom: 20px;
            }
            </style>
        """, unsafe_allow_html=True)

        # Formulário para evitar rerun completo
        with st.form("form_calc"):
            col_cod, col_ajuste = st.columns([2, 1])
            cod_calc = col_cod.text_input("Código do Procedimento", placeholder="Ex: 10101012", key="in_calc")
            infla = col_ajuste.number_input("Ajuste Adicional (%)", 0.0, step=0.5, key="in_infla")

            c1, c2 = st.columns(2)
            uco_v = c1.number_input("Valor UCO (R$)", 1.0, step=0.01, format="%.4f", key="in_uco_val")
            filme_v = c2.number_input("Valor Filme (R$)", 21.70, step=0.01, format="%.2f", key="in_filme_val")

            # Botão dentro do form evita rerun completo
            calcular_btn = st.form_submit_button("Calcular Agora")

        if calcular_btn:
            st.session_state.aba_ativa_idx = 2  # mantém aba de cálculo
            if not cod_calc:
                st.warning("Por favor, insira um código.")
            else:
                res = buscar_dados(cod_calc, v_selecionada, "Código")
                if not res.empty:
                    p = res.iloc[0]
                    f = 1 + (infla/100)
                    
                    porte_calc = p['porte'] * f
                    uco_calc = p['uco'] * uco_v * f
                    filme_calc = p['filme'] * filme_v * f
                    total = porte_calc + uco_calc + filme_calc

                    # Resultado visual
                    st.markdown(f"""
                        <div class="res-card">
                            <small>Procedimento encontrado em <b>{v_selecionada}</b></small><br>
                            <span style='font-size: 1.2rem;'>{p['descricao']}</span>
                        </div>
                    """, unsafe_allow_html=True)

                    c_porte, c_uco, c_filme, c_total = st.columns(4)
                    c_porte.metric("Porte", f"R$ {porte_calc:,.2f}")
                    c_uco.metric("UCO", f"R$ {uco_calc:,.2f}")
                    c_filme.metric("Filme", f"R$ {filme_calc:,.2f}")
                    c_total.metric("TOTAL FINAL", f"R$ {total:,.2f}", delta=f"{infla:.2f}%" if infla != 0 else None)
                    
                    st.divider()

                else:
                    st.error(f"O código '{cod_calc}' não foi encontrado na tabela {v_selecionada}.")
        
# --- 4. COMPARAR ---
with abas[3]:
    if len(lista_v) >= 2:
        # Seleciona as versões
        col1, col2 = st.columns(2)
        v1 = col1.selectbox("Versão Anterior", lista_v, key="v1")
        v2 = col2.selectbox("Versão Atual", lista_v, key="v2")
        
        # Botão de análise: mantém a aba ativa
        if st.button("Analisar Reajustes"):
            st.session_state.comparacao_realizada = True
            st.session_state.aba_ativa_idx = 3  # salva a aba ativa

        # Mantém a aba ativa mesmo após rerun
        if st.session_state.comparacao_realizada:
            st.session_state.aba_ativa_idx = 3  # reforça aba ativa
            df1 = buscar_dados("", v1, "Código")
            df2 = buscar_dados("", v2, "Código").rename(
                columns={"porte":"porte_2", "uco":"uco_2", "filme":"filme_2", "descricao":"desc_2"}
            )
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
    else:
        st.warning("Necessário ao menos 2 versões para comparar.")


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
