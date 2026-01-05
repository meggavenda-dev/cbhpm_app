import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os
import requests
import base64
import hashlib
from io import BytesIO
from contextlib import contextmanager
import time
import altair as alt

# =====================================================
# CONFIGURAÇÕES E ESTADO DA SESSÃO
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

if "aba_ativa" not in st.session_state:
    st.session_state.aba_ativa = 0

if 'comparacao_realizada' not in st.session_state:
    st.session_state.comparacao_realizada = False

# =====================================================
# CONEXÃO E LIMPEZA (Context Manager)
# =====================================================
@contextmanager
def gerenciar_db():
    con = sqlite3.connect(DB_NAME, check_same_thread=False, timeout=20)
    try:
        yield con
        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        con.close()

# =====================================================
# UTILITÁRIOS
# =====================================================
def to_float(v):
    try:
        if pd.isna(v) or v == "": return 0.0
        if isinstance(v, str):
            v = v.replace(".", "").replace(",", ".").strip()
        return float(v)
    except:
        return 0.0

def gerar_hash_arquivo(uploaded_file):
    uploaded_file.seek(0)
    h = hashlib.sha256(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h

# =====================================================
# IMPORTAÇÃO COM FEEDBACK
# =====================================================
def importar(arquivos, versao):
    if not versao:
        st.error("Por favor, informe a Versão CBHPM.")
        return False
    
    # Mapeamento flexível (insensível a acentos/caixa)
    mapa = {
        "codigo": ["código", "codigo", "cod"],
        "descricao": ["descrição", "descricao", "desc"],
        "porte": ["porte"],
        "uco": ["uco", "ch"],
        "filme": ["filme"]
    }
    
    arquivos_processados = 0
    progresso = st.progress(0)
    
    with gerenciar_db() as con:
        cur = con.cursor()
        for idx, arq in enumerate(arquivos):
            h = gerar_hash_arquivo(arq)
            if arquivo_ja_importado(h):
                st.warning(f"O conteúdo de '{arq.name}' já foi importado.")
                continue
            
            # Carregamento inteligente
            df = pd.read_csv(arq, sep=";", encoding="utf-8") if arq.name.lower().endswith(".csv") else pd.read_excel(arq)
            df.columns = [c.strip().lower() for c in df.columns]
            
            dados_lista = []
            for _, row in df.iterrows():
                # Busca coluna correspondente no mapa
                vals = {}
                for campo, aliases in mapa.items():
                    col = next((c for c in aliases if c in df.columns), None)
                    vals[campo] = row[col] if col else (0.0 if campo != "descricao" else "")
                
                dados_lista.append((
                    str(vals["codigo"]), 
                    str(vals["descricao"]), 
                    to_float(vals["porte"]), 
                    to_float(vals["uco"]), 
                    to_float(vals["filme"]), 
                    versao
                ))
            
            cur.executemany("INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao) VALUES (?, ?, ?, ?, ?, ?)", dados_lista)
            cur.execute("INSERT OR IGNORE INTO arquivos_importados (hash, versao, data) VALUES (?, ?, ?)", (h, versao, datetime.now().isoformat()))
            arquivos_processados += 1
            progresso.progress((idx + 1) / len(arquivos))
            
    return arquivos_processados > 0

# =====================================================
# CONSULTAS
# =====================================================
@st.cache_data
def versoes():
    with sqlite3.connect(DB_NAME) as con:
        try: 
            return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except: 
            return []

def buscar_dados(termo, versao, tipo, capitulo="Todos"):
    campo = "codigo" if tipo == "Código" else "descricao"
    query = f"SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE {campo} LIKE ? AND versao = ?"
    params = [f"%{termo}%", versao]
    
    if capitulo != "Todos":
        query += " AND codigo LIKE ?"
        params.append(f"{capitulo}%")
        
    with sqlite3.connect(DB_NAME) as con:
        return pd.read_sql(query, con, params=params)

# =====================================================
# INTERFACE PRINCIPAL
# =====================================================
st.set_page_config(page_title="CBHPM Gestão", layout="wide")
st.title("CBHPM • Gestão Inteligente")

# Sidebar com filtros globais
with st.sidebar:
    st.header("Configurações")
    lista_v = versoes()
    v_selecionada = st.selectbox("Tabela Ativa", lista_v) if lista_v else None
    
    capitulos = ["Todos", "10", "20", "30", "40"] # Exemplo de capítulos CBHPM
    filtro_cap = st.selectbox("Filtrar Capítulo", capitulos)

abas_nome = ["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar"]
abas = st.tabs(abas_nome)

# --- 1. IMPORTAR ---
with abas[0]:
    st.session_state.aba_ativa = 0
    v_imp = st.text_input("Nome da Versão", key="txt_v_imp")
    arqs = st.file_uploader("Arquivos", accept_multiple_files=True, key="file_up_imp")
    if st.button("Executar Importação", use_container_width=True):
        if importar(arqs, v_imp):
            st.success("Sucesso!")
            st.balloons()
            st.cache_data.clear()

# --- 2. CONSULTAR ---
with abas[1]:
    st.session_state.aba_ativa = 1
    if v_selecionada:
        c1, c2 = st.columns([1, 3])
        tipo = c1.radio("Buscar por", ["Código", "Descrição"])
        termo = c2.text_input("Termo de busca")
        res = buscar_dados(termo, v_selecionada, tipo, filtro_cap)
        st.dataframe(res, use_container_width=True, hide_index=True)

# --- 4. COMPARAR ---
with abas[3]:
    st.session_state.aba_ativa = 3
    if len(lista_v) >= 2:
        col_v1, col_v2 = st.columns(2)
        va = col_v1.selectbox("Base (Antiga)", lista_v, key="va_comp")
        vb = col_v2.selectbox("Comparação (Nova)", lista_v, key="vb_comp")
        
        if st.button("Analisar Reajustes", use_container_width=True):
            st.session_state.comparacao_realizada = True
            
        if st.session_state.comparacao_realizada:
            dfa = buscar_dados("", va, "Código", filtro_cap)
            dfb = buscar_dados("", vb, "Código", filtro_cap).rename(columns={"porte":"porte_B","uco":"uco_B","filme":"filme_B", "descricao":"desc_B"})
            comp = dfa.merge(dfb, on="codigo")
            
            if not comp.empty:
                comp['perc_var'] = ((comp['porte_B'] - comp['porte']) / comp['porte'].replace(0,1)) * 100
                
                # Visualização de Impacto
                st.subheader("Resumo do Reajuste")
                m1, m2, m3 = st.columns(3)
                m1.metric("Itens Analisados", len(comp))
                m2.metric("Variação Média", f"{comp['perc_var'].mean():.2f}%")
                m3.metric("Com Aumento", len(comp[comp['perc_var'] > 0]))

                # Gráfico
                chart = alt.Chart(comp).mark_bar().encode(
                    x=alt.X('codigo:N', sort='-y'),
                    y='perc_var:Q',
                    color=alt.condition(alt.datum.perc_var > 0, alt.value('#2ecc71'), alt.value('#e74c3c')),
                    tooltip=['codigo', 'descricao', 'perc_var']
                ).properties(height=350)
                st.altair_chart(chart, use_container_width=True)
