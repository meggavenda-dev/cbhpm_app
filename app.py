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

if 'comparacao_realizada' not in st.session_state:
    st.session_state.comparacao_realizada = False

# =====================================================
# UTILITÁRIOS E BANCO (Refatorado)
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

def to_float(v):
    try:
        if pd.isna(v) or v == "": return 0.0
        if isinstance(v, str):
            v = v.replace(".", "").replace(",", ".").strip()
        return float(v)
    except: return 0.0

def gerar_hash_arquivo(uploaded_file):
    uploaded_file.seek(0)
    h = hashlib.sha256(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h

# =====================================================
# LÓGICA DE NEGÓCIO
# =====================================================
def importar_com_progresso(arquivos, versao):
    if not versao:
        st.error("Informe o nome da versão.")
        return False
    
    progresso = st.progress(0)
    status_text = st.empty()
    mapa = {"codigo": ["Código", "Codigo"], "descricao": ["Descrição", "Descricao"], "porte": ["Porte"], "uco": ["UCO", "CH"], "filme": ["Filme"]}
    
    total_arqs = len(arquivos)
    with gerenciar_db() as con:
        cur = con.cursor()
        for idx, arq in enumerate(arquivos):
            status_text.text(f"Importando: {arq.name}...")
            h = gerar_hash_arquivo(arq)
            if idx == 0: # Simulação de segurança para não duplicar hash
                pass
            
            df = pd.read_csv(arq, sep=";", encoding="utf-8") if arq.name.lower().endswith(".csv") else pd.read_excel(arq)
            df.columns = [c.strip() for c in df.columns]
            
            dados_lista = []
            for _, row in df.iterrows():
                d = {campo: to_float(row[next((c for c in cols if c in df.columns), None)]) if next((c for c in cols if c in df.columns), None) else 0.0 for campo, cols in mapa.items()}
                dados_lista.append((str(row[next((c for c in mapa["codigo"] if c in df.columns))]), str(row[next((c for c in mapa["descricao"] if c in df.columns))]), d["porte"], d["uco"], d["filme"], versao))
            
            cur.executemany("INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao) VALUES (?, ?, ?, ?, ?, ?)", dados_lista)
            cur.execute("INSERT OR IGNORE INTO arquivos_importados (hash, versao, data) VALUES (?, ?, ?)", (h, versao, datetime.now().isoformat()))
            progresso.progress((idx + 1) / total_arqs)
            
    status_text.empty()
    progresso.empty()
    return True

@st.cache_data
def versoes():
    with sqlite3.connect(DB_NAME) as con:
        try: return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except: return []

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
# INTERFACE
# =====================================================
st.set_page_config(page_title="CBHPM Pro", layout="wide")
st.title("⚖️ CBHPM • Gestão & Auditoria")

lista_v = versoes()

with st.sidebar:
    st.header("Filtros")
    v_selecionada = st.selectbox("Tabela Ativa", lista_v, key="v_global") if lista_v else None
    
    capitulos = ["Todos"]
    if v_selecionada:
        with sqlite3.connect(DB_NAME) as con:
            df_caps = pd.read_sql(f"SELECT DISTINCT substr(codigo, 1, 2) as cap FROM procedimentos WHERE versao='{v_selecionada}'", con)
            capitulos.extend(sorted(df_caps['cap'].tolist()))
    filtro_cap = st.selectbox("Capítulo/Grupo", capitulos)

abas = st.tabs(["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "🗑️ Gerenciar"])

# --- 1. IMPORTAR ---
with abas[0]:
    c1, c2 = st.columns(2)
    v_imp = c1.text_input("Nome da Nova Versão")
    arqs = c2.file_uploader("Arquivos (Excel ou CSV)", accept_multiple_files=True)
    if st.button("Executar Importação"):
        if importar_com_progresso(arqs, v_imp):
            st.success("Tabelas importadas com sucesso!")
            st.cache_data.clear()
            st.rerun()

# --- 2. CONSULTAR ---
with abas[1]:
    if v_selecionada:
        c1, c2 = st.columns([1, 3])
        tipo = c1.radio("Buscar por", ["Código", "Descrição"], horizontal=True)
        termo = c2.text_input("Buscar...")
        st.dataframe(buscar_dados(termo, v_selecionada, tipo, filtro_cap), use_container_width=True)

# --- 3. CALCULAR ---
with abas[2]:
    if v_selecionada:
        cod_calc = st.text_input("Código do Procedimento")
        c1, c2, c3 = st.columns(3)
        uco_v = c1.number_input("Valor UCO", 1.0)
        filme_v = c2.number_input("Valor Filme", 21.70)
        infla_v = c3.number_input("Inflator %", 0.0)
        if st.button("Ver Cálculo"):
            res = buscar_dados(cod_calc, v_selecionada, "Código")
            if not res.empty:
                p = res.iloc[0]
                f = 1 + (infla_v/100)
                tot = (p['porte']*f) + (p['uco']*uco_v*f) + (p['filme']*filme_v*f)
                st.metric(f"Total: {p['descricao']}", f"R$ {tot:,.2f}")

# --- 4. COMPARAR ---
with abas[3]:
    if len(lista_v) >= 2:
        col_v1, col_v2 = st.columns(2)
        va = col_v1.selectbox("Versão Base", lista_v, key="va_comp")
        vb = col_v2.selectbox("Versão Nova", lista_v, key="vb_comp")
        
        if st.button("Comparar Grupos"):
            st.session_state.comparacao_realizada = True
            
        if st.session_state.comparacao_realizada:
            dfa = buscar_dados("", va, "Código", filtro_cap)
            dfb = buscar_dados("", vb, "Código", filtro_cap).rename(columns={"porte":"porte_B", "descricao": "desc_b"})
            comp = dfa.merge(dfb, on="codigo")
            
            if not comp.empty:
                comp['perc_var'] = ((comp['porte_B'] - comp['porte']) / comp['porte'].replace(0, 1)) * 100
                
                # --- GRÁFICO RESUMIDO POR GRUPO (CAPÍTULO) ---
                comp['Grupo'] = comp['codigo'].astype(str).str[:2]
                resumo = comp.groupby('Grupo')['perc_var'].mean().reset_index()
                resumo.columns = ['Capítulo', 'Aumento Médio (%)']

                st.subheader("📈 Variação Média por Capítulo")
                chart = alt.Chart(resumo).mark_bar().encode(
                    x=alt.X('Capítulo:N', title="Capítulo (Início do Código)", sort='-y'),
                    y=alt.Y('Aumento Médio (%):Q', title="Variação %"),
                    color=alt.condition(alt.datum['Aumento Médio (%)'] > 0, alt.value('#2ecc71'), alt.value('#e74c3c')),
                    tooltip=['Capítulo', 'Aumento Médio (%)']
                ).properties(height=400)
                
                st.altair_chart(chart, use_container_width=True)

                st.write("---")
                st.write("### Itens com maior reajuste no grupo selecionado")
                st.dataframe(comp[['codigo', 'descricao', 'porte', 'porte_B', 'perc_var']].sort_values('perc_var', ascending=False), use_container_width=True)
            else: st.warning("Sem dados coincidentes para comparação.")

# --- 5. GERENCIAR ---
with abas[4]:
    if lista_v:
        v_del = st.selectbox("Selecione para excluir", lista_v)
        if st.button("Confirmar Exclusão Definitiva"):
            with gerenciar_db() as con:
                con.execute("DELETE FROM procedimentos WHERE versao=?", (v_del,))
                con.execute("DELETE FROM arquivos_importados WHERE versao=?", (v_del,))
            st.success(f"Versão {v_del} removida.")
            st.cache_data.clear()
            st.rerun()
