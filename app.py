import os
import base64
import hashlib
import time
from io import BytesIO
from contextlib import contextmanager
from datetime import datetime

import pandas as pd
import requests
import sqlite3
import altair as alt
import streamlit as st

# =====================================================
# CONFIGURAÇÕES E ESTADO DA SESSÃO
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

if 'comparacao_realizada' not in st.session_state:
    st.session_state.comparacao_realizada = False
    
# =====================================================
# CONEXÃO COM SQLITE (cacheada)
# =====================================================
@st.cache_resource
def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False, timeout=20)

@contextmanager
def gerenciar_db():
    con = get_connection()
    try:
        yield con
        con.commit()
    except Exception as e:
        con.rollback()
        raise e

# =====================================================
# UTILITÁRIOS
# =====================================================
def to_float(v):
    if pd.isna(v) or v == "":
        return 0.0
    if isinstance(v, str):
        v = v.replace(".", "").replace(",", ".").strip()
    try:
        return float(v)
    except ValueError:
        return 0.0

def gerar_hash_arquivo(uploaded_file):
    uploaded_file.seek(0)
    h = hashlib.sha256(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h

def extrair_valor(row, df, col_opts):
    for c in col_opts:
        if c in df.columns:
            return to_float(row[c])
    return 0.0

# =====================================================
# GITHUB – PERSISTÊNCIA
# =====================================================
def github_request(method, url, **kwargs):
    headers = {
        "Authorization": f"token {st.secrets['GITHUB_TOKEN']}",
        "Accept": "application/vnd.github.v3+json"
    }
    try:
        r = requests.request(method, url, headers=headers, **kwargs)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        st.error(f"Erro GitHub: {e}")
        return None

def baixar_banco():
    if os.path.exists(DB_NAME):
        return
    try:
        url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        r = github_request("GET", url)
        if r and r.status_code == 200:
            content = r.json()["content"]
            with open(DB_NAME, "wb") as f:
                f.write(base64.b64decode(content))
        else:
            open(DB_NAME, "wb").close()
    except Exception as e:
        st.error(f"Erro ao baixar banco: {e}")

def salvar_banco_github(msg):
    try:
        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()
        api_url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        r = github_request("GET", api_url)
        sha = r.json().get("sha") if r and r.status_code == 200 else None
        payload = {"message": msg, "content": content, "branch": st.secrets["GITHUB_BRANCH"]}
        if sha:
            payload["sha"] = sha
        github_request("PUT", api_url, json=payload)
    except:
        st.warning("Erro na sincronização com GitHub.")

# =====================================================
# BANCO DE DADOS
# =====================================================
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS arquivos_importados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                versao TEXT,
                data TEXT
            )
        """)

def arquivo_ja_importado(h):
    with get_connection() as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM arquivos_importados WHERE hash=?", (h,))
        return cur.fetchone() is not None

def registrar_arquivo(h, versao):
    with gerenciar_db() as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO arquivos_importados VALUES (NULL,?,?,?)", (h, versao, datetime.now().isoformat()))

def excluir_versao(versao):
    with gerenciar_db() as con:
        cur = con.cursor()
        # Remove os dados da tabela de procedimentos
        cur.execute("DELETE FROM procedimentos WHERE versao=?", (versao,))
        total = cur.rowcount
        # Remove o registro do arquivo para permitir re-importação futura
        cur.execute("DELETE FROM arquivos_importados WHERE versao=?", (versao,))
    
    # Sincroniza o arquivo .db com o GitHub após a alteração
    salvar_banco_github(f"Exclusão da versão {versao}")
    return total

# =====================================================
# IMPORTAÇÃO (CORRIGIDA PARA ERRO DE ENCODING)
# =====================================================
def importar(arquivos, versao):
    if not versao:
        st.error("Por favor, informe a Versão CBHPM.")
        return False

    mapa = {
        "codigo": ["Código", "Codigo"],
        "descricao": ["Descrição", "Descricao"],
        "porte": ["Porte"],
        "uco": ["UCO", "CH"],
        "filme": ["Filme"]
    }

    arquivos_processados = 0
    with gerenciar_db() as con:
        cur = con.cursor()
        for arq in arquivos:
            h = gerar_hash_arquivo(arq)
            if arquivo_ja_importado(h):
                st.warning(f"O conteúdo de '{arq.name}' já foi importado.")
                continue

            # --- CORREÇÃO DE ENCODING AQUI ---
            try:
                if arq.name.lower().endswith(".csv"):
                    try:
                        # Tenta UTF-8 primeiro
                        df = pd.read_csv(arq, sep=";", encoding="utf-8")
                    except UnicodeDecodeError:
                        # Se falhar, tenta Latin-1 (comum em arquivos BR)
                        arq.seek(0)
                        df = pd.read_csv(arq, sep=";", encoding="latin-1")
                else:
                    df = pd.read_excel(arq)
            except Exception as e:
                st.error(f"Erro ao ler o arquivo {arq.name}: {e}")
                continue
            # ---------------------------------

            df.columns = [c.strip() for c in df.columns]

            dados_lista = []
            for _, row in df.iterrows():
                d = {campo: extrair_valor(row, df, cols) for campo, cols in mapa.items()}
                codigo = str(row[next((c for c in mapa["codigo"] if c in df.columns))])
                descricao = str(row[next((c for c in mapa["descricao"] if c in df.columns))])
                dados_lista.append((codigo, descricao, d["porte"], d["uco"], d["filme"], versao))

            cur.executemany(
                "INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao) VALUES (?, ?, ?, ?, ?, ?)",
                dados_lista
            )
            cur.execute(
                "INSERT OR IGNORE INTO arquivos_importados (hash, versao, data) VALUES (?, ?, ?)",
                (h, versao, datetime.now().isoformat())
            )
            arquivos_processados += 1

    if arquivos_processados > 0:
        salvar_banco_github(f"Importação {versao}")
        return True

    return False

# =====================================================
# CONSULTAS
# =====================================================
@st.cache_data
def versoes():
    with get_connection() as con:
        try:
            return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except:
            return []

def buscar_dados(termo, versao, tipo):
    campo = "codigo" if tipo == "Código" else "descricao"
    with get_connection() as con:
        return pd.read_sql(
            f"SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE {campo} LIKE ? AND versao = ?",
            con,
            params=(f"%{termo}%", versao)
        )

# =====================================================
# INTERFACE STREAMLIT
# =====================================================
baixar_banco()
lista_versoes = versoes()
v_selecionada = st.sidebar.selectbox("Tabela CBHPM Ativa", lista_versoes, key="v_global") if lista_versoes else None
abas = st.tabs(["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar"])

# --- 1. IMPORTAR ---
with abas[0]:
    v_imp = st.text_input("Nome da Versão (ex: CBHPM 2024)", key="txt_v_imp")
    arqs = st.file_uploader("Upload arquivos", accept_multiple_files=True, key="file_up_imp")
    if st.button("Executar Importação", key="btn_importar_final"):
        if importar(arqs, v_imp):
            st.success(f"Tabela '{v_imp}' importada!")
            st.balloons()
            st.cache_data.clear()  # limpa qualquer cache residual
            lista_versoes = versoes()  # atualiza imediatamente
            st.experimental_rerun()   # força atualização do sidebar

# --- 2. CONSULTAR ---
with abas[1]:
    if v_selecionada:
        st.info(f"Versão Ativa na Sidebar: {v_selecionada}")
        c1, c2 = st.columns([1, 3])
        tipo = c1.radio("Tipo", ["Código", "Descrição"], key="radio_tipo_busca")
        termo = c2.text_input("Termo", key="input_termo_busca")
        if st.button("Buscar", key="btn_buscar_consulta"):
            st.dataframe(buscar_dados(termo, v_selecionada, tipo), use_container_width=True)

# --- 3. CALCULAR ---
with abas[2]:
    if v_selecionada:
        cod_calc = st.text_input("Código", key="input_cod_calc")
        col1, col2, col3 = st.columns(3)
        uco_v = col1.number_input("Valor UCO", 1.0, key="input_uco")
        filme_v = col2.number_input("Valor Filme", 21.70, key="input_filme")
        infla_v = col3.number_input("Acréscimo %", 0.0, key="input_infla")
        if st.button("Calcular", key="btn_calcular_total"):
            res = buscar_dados(cod_calc, v_selecionada, "Código")
            if not res.empty:
                p = res.iloc[0]
                f = 1 + (infla_v/100)
                tot = (p['porte']*f) + (p['uco']*uco_v*f) + (p['filme']*filme_v*f)
                st.metric(p['descricao'], f"R$ {tot:,.2f}")

# --- 4. COMPARAR ---
with abas[3]:
    if len(lista_versoes) >= 2:
        # ... (seu código de seleção de va e vb) ...
        
        if st.session_state.comparacao_realizada:
            # ... (seu código de merge e cálculo de perc_var) ...
            
            if not comp.empty:
                st.subheader("📊 Análise de Reajustes")
                
                # Exibição da tabela configurada para não cortar dados
                st.dataframe(
                    comp[['codigo', 'descricao', 'porte', 'porte_B', 'perc_var']], 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "codigo": st.column_config.TextColumn("Código", width="small"),
                        "descricao": st.column_config.TextColumn(
                            "Descrição do Procedimento", 
                            width="large" # Isso impede que a descrição corte
                        ),
                        "porte": st.column_config.NumberColumn(f"Porte ({va})", format="%.2f"),
                        "porte_B": st.column_config.NumberColumn(f"Porte ({vb})", format="%.2f"),
                        "perc_var": st.column_config.NumberColumn(
                            "Variação %", 
                            format="%.2f%%"
                        )
                    }
                )
            else:
                st.warning("Nenhuma coincidência encontrada entre as tabelas.")

# --- 5. EXPORTAR ---
with abas[4]:
    if st.button("Gerar Arquivo Excel", key="btn_export_xlsx"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            with get_connection() as con:
                pd.read_sql("SELECT * FROM procedimentos", con).to_excel(writer, index=False)
        st.download_button("Clique aqui para baixar", output.getvalue(), "cbhpm_export.xlsx", key="dl_btn")

# --- 6. GERENCIAR ---
with abas[5]: # ABA GERENCIAR
    if lista_versoes:
        st.subheader("🗑️ Gerenciar Dados")
        
        # Criamos um container para as mensagens de sucesso/erro
        placeholder = st.empty()
        
        v_del = st.selectbox("Selecione a versão para excluir permanentemente", lista_versoes, key="v_del_aba")
        confirmar = st.checkbox(f"Confirmo que desejo apagar todos os registros da versão {v_del}")
        
        if st.button("Confirmar Exclusão Definitiva", type="primary", key="btn_confirm_del"):
            if confirmar:
                # 1. Executa a exclusão no banco e GitHub
                n_removidos = excluir_versao(v_del)
                
                # 2. Limpa o cache IMEDIATAMENTE
                st.cache_data.clear()
                
                # 3. Feedback visual no placeholder
                placeholder.success(f"Sucesso! Versão '{v_del}' e seus {n_removidos} registros foram removidos.")
                
                # 4. Pequena pausa para o usuário ler e REINÍCIO FORÇADO
                time.sleep(1.5)
                st.rerun() 
            else:
                st.warning("Marque a caixa de confirmação para prosseguir.")
    else:
        st.info("Nenhuma versão encontrada no banco de dados.")
