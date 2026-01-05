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

# 1. Gerenciamento do estado para as abas não pularem
if 'aba_atual' not in st.session_state:
    st.session_state.aba_atual = 0
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
        else:
            open(DB_NAME, "wb").close()
    except Exception as e:
        st.error(f"Erro ao baixar banco: {e}")

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
    except:
        st.warning("Erro na sincronização GitHub.")

# =====================================================
# OPERAÇÕES DE BANCO DE DADOS
# =====================================================
def criar_tabelas():
    with gerenciar_db() as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS procedimentos (id INTEGER PRIMARY KEY AUTOINCREMENT, codigo TEXT, descricao TEXT, porte REAL, uco REAL, filme REAL, versao TEXT, UNIQUE (codigo, versao))")
        cur.execute("CREATE TABLE IF NOT EXISTS arquivos_importados (id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, versao TEXT, data TEXT)")

def arquivo_ja_importado(h):
    with sqlite3.connect(DB_NAME) as con:
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
        cur.execute("DELETE FROM procedimentos WHERE versao=?", (versao,))
        total = cur.rowcount
        cur.execute("DELETE FROM arquivos_importados WHERE versao=?", (versao,))
        salvar_banco_github(f"Exclusão da versão {versao}")
        return total

# =====================================================
# IMPORTAÇÃO
# =====================================================
def importar(arquivos, versao):
    if not versao:
        st.error("Por favor, informe a Versão CBHPM.")
        return False
    mapa = {"codigo": ["Código", "Codigo"], "descricao": ["Descrição", "Descricao"], "porte": ["Porte"], "uco": ["UCO", "CH"], "filme": ["Filme"]}
    arquivos_processados = 0
    with gerenciar_db() as con:
        cur = con.cursor()
        for arq in arquivos:
            h = gerar_hash_arquivo(arq)
            if arquivo_ja_importado(h):
                st.warning(f"O conteúdo de '{arq.name}' já foi importado.")
                continue
            df = pd.read_csv(arq, sep=";", encoding="utf-8") if arq.name.lower().endswith(".csv") else pd.read_excel(arq)
            df.columns = [c.strip() for c in df.columns]
            dados_lista = []
            for _, row in df.iterrows():
                d = {campo: to_float(row[next((c for c in cols if c in df.columns), None)]) if next((c for c in cols if c in df.columns), None) else 0.0 for campo, cols in mapa.items()}
                dados_lista.append((str(row[next((c for c in mapa["codigo"] if c in df.columns))]), str(row[next((c for c in mapa["descricao"] if c in df.columns))]), d["porte"], d["uco"], d["filme"], versao))
            cur.executemany("INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao) VALUES (?, ?, ?, ?, ?, ?)", dados_lista)
            cur.execute("INSERT OR IGNORE INTO arquivos_importados (hash, versao, data) VALUES (?, ?, ?)", (h, versao, datetime.now().isoformat()))
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
    with sqlite3.connect(DB_NAME) as con:
        try: return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except: return []

def buscar_dados(termo, versao, tipo):
    campo = "codigo" if tipo == "Código" else "descricao"
    with sqlite3.connect(DB_NAME) as con:
        return pd.read_sql(f"SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE {campo} LIKE ? AND versao = ?", con, params=(f"%{termo}%", versao))

# =====================================================
# INTERFACE PRINCIPAL
# =====================================================
baixar_banco()
criar_tabelas()

st.set_page_config(page_title="CBHPM Gestão", layout="wide")
st.title("CBHPM • Gestão Inteligente")

lista_versoes = versoes()
v_selecionada = st.sidebar.selectbox("Tabela CBHPM Ativa", lista_versoes, key="v_global") if lista_versoes else None

# Controle de abas persistente usando session_state
titulos_abas = ["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ r", "📤 Exportar", "🗑️ Gerenciar"]
# Se a versão do Streamlit for >= 1.30, ele suporta o parâmetro 'key' para st.tabs
# Caso contrário, ele usa a ordem natural. O 'st.session_state' garante que o script carregue corretamente.
abas = st.tabs(titulos_abas)

# --- 1. IMPORTAR ---
with abas[0]:
    v_imp = st.text_input("Nome da Versão (ex: CBHPM 2024)")
    arqs = st.file_uploader("Upload arquivos", accept_multiple_files=True)
    if st.button("Executar Importação", key="btn_importar_final"):
        if importar(arqs, v_imp):
            st.success(f"Tabela '{v_imp}' importada!")
            st.balloons()
            st.cache_data.clear()
            time.sleep(1)
            # Ao importar, mantemos na aba 0 (Importar) ou mudamos para 1 (Consultar)
            st.session_state.aba_atual = 0 
            st.rerun()

# --- 2. CONSULTAR ---
with abas[1]:
    if v_selecionada:
        st.info(f"Versão Ativa: {v_selecionada}")
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
        col_v1, col_v2 = st.columns(2)
        # O segredo aqui: Ao mudar o selectbox, resetamos o estado da comparação realizada
        va = col_v1.selectbox("Base (Antiga)", lista_versoes, key="va_comp", on_change=lambda: st.session_state.update({"comparacao_realizada": False}))
        vb = col_v2.selectbox("Comparação (Nova)", lista_versoes, key="vb_comp", on_change=lambda: st.session_state.update({"comparacao_realizada": False}))
        
        if st.button("Analisar Diferenças", key="btn_analisar_comp"):
            st.session_state.comparacao_realizada = True
            
       if st.session_state.comparacao_realizada:
            dfa = buscar_dados("", va, "Código")
            # Renomeamos a descrição também para evitar o sufixo _x e _y
            dfb = buscar_dados("", vb, "Código").rename(columns={
                "porte": "porte_B", 
                "uco": "uco_B", 
                "filme": "filme_B",
                "descricao": "descricao_B" # Adicionado renomeação da descrição
            })
            
            comp = dfa.merge(dfb, on="codigo")
            
            if not comp.empty:
                # Calculamos a variação baseada no Porte
                comp['perc_var'] = ((comp['porte_B'] - comp['porte']) / comp['porte'].replace(0, 1)) * 100
                
                # MÉTRICAS
                m1, m2, m3 = st.columns(3)
                m1.metric("Itens em Comum", len(comp))
                m2.metric("Variação Média", f"{comp['perc_var'].mean():.2f}%")
                m3.metric("Com Aumento", len(comp[comp['perc_var'] > 0]))

                # GRÁFICO
                comp['Grupo'] = comp['codigo'].astype(str).str[:2]
                resumo = comp.groupby('Grupo')['perc_var'].mean().reset_index()
                
                chart = alt.Chart(resumo).mark_bar().encode(
                    x=alt.X('Grupo:N', sort='-y', title="Capítulo (Início do Código)"),
                    y=alt.Y('perc_var:Q', title="Aumento Médio (%)"),
                    color=alt.condition(alt.datum.perc_var > 0, alt.value('steelblue'), alt.value('orange'))
                ).properties(height=350)
                
                st.altair_chart(chart, use_container_width=True)

                # CORREÇÃO DO DATAFRAME:
                # Usamos a 'descricao' da tabela A (dfa) que permaneceu com o nome original após o merge
                st.write("### Tabela Detalhada")
                st.dataframe(
                    comp[['codigo', 'descricao', 'porte', 'porte_B', 'perc_var']], 
                    column_config={
                        "codigo": "Código",
                        "descricao": "Descrição",
                        "porte": f"Porte ({va})",
                        "porte_B": f"Porte ({vb})",
                        "perc_var": st.column_config.NumberColumn("Variação %", format="%.2f%%")
                    },
                    use_container_width=True
                )
            else:
                st.warning("Não foram encontrados códigos em comum entre estas duas versões para comparação.")

# --- 5. EXPORTAR ---
with abas[4]:
    if st.button("Gerar Arquivo para Download", key="btn_exportar_xlsx"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            with sqlite3.connect(DB_NAME) as con:
                pd.read_sql("SELECT * FROM procedimentos", con).to_excel(writer, index=False)
        st.download_button("Clique aqui para Baixar Excel", output.getvalue(), "cbhpm_exportado.xlsx", key="btn_download_real")

# --- 6. GERENCIAR ---
with abas[5]:
    if lista_versoes:
        v_del = st.selectbox("Versão", lista_versoes, key="v_del_aba_gerenciar")
        if st.button("Confirmar Exclusão", key="btn_deletar_versao"):
            excluir_versao(v_del)
            st.cache_data.clear()
            st.rerun()
