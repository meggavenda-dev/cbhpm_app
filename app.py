import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os
import requests
import base64
import hashlib
from io import BytesIO

# =====================================================
# CONFIGURAÇÕES
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

# =====================================================
# CONEXÃO E UTILITÁRIOS
# =====================================================
def conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def to_float(v):
    try:
        if pd.isna(v): return 0.0
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
        st.warning("Erro na sincronização GitHub. Dados salvos apenas localmente.")

# =====================================================
# BANCO DE DADOS
# =====================================================
def criar_tabelas():
    con = conn()
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
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS arquivos_importados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT UNIQUE,
        versao TEXT,
        data TEXT
    )""")
    con.commit()
    con.close()

def arquivo_ja_importado(h):
    con = conn()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM arquivos_importados WHERE hash=?", (h,))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def registrar_arquivo(h, versao):
    con = conn()
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO arquivos_importados VALUES (NULL,?,?,?)", 
                (h, versao, datetime.now().isoformat()))
    con.commit()
    con.close()

def excluir_versao(versao):
    con = conn()
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM procedimentos WHERE versao=?", (versao,))
        total = cur.rowcount
        cur.execute("DELETE FROM arquivos_importados WHERE versao=?", (versao,))
        con.commit()
        salvar_banco_github(f"Exclusão da versão {versao}")
        return total
    except Exception as e:
        con.rollback()
        raise e
    finally:
        con.close()

# =====================================================
# IMPORTAÇÃO
# =====================================================
def importar(arquivos, versao):
    if not versao:
        st.error("Por favor, informe a Versão CBHPM.")
        return

    mapa = {"codigo": ["Código", "Codigo"], "descricao": ["Descrição", "Descricao"], 
            "porte": ["Porte"], "uco": ["UCO", "CH"], "filme": ["Filme"]}
    
    con = conn()
    cur = con.cursor()

    for arq in arquivos:
        h = gerar_hash_arquivo(arq)
        if arquivo_ja_importado(h):
            st.info(f"O arquivo {arq.name} já foi importado.")
            continue

        try:
            if arq.name.lower().endswith(".csv"):
                try:
                    df = pd.read_csv(arq, sep=";", encoding="utf-8")
                except UnicodeDecodeError:
                    arq.seek(0)
                    df = pd.read_csv(arq, sep=";", encoding="latin-1")
            else:
                df = pd.read_excel(arq)
            
            df.columns = [c.strip() for c in df.columns]
            dados = {campo: (df[next((c for c in cols if c in df.columns), None)] 
                     if next((c for c in cols if c in df.columns), None) else 0) 
                     for campo, cols in mapa.items()}

            df_f = pd.DataFrame(dados)
            df_f["versao"] = versao

            for c in ["porte", "uco", "filme"]: df_f[c] = df_f[c].apply(to_float)

            for _, r in df_f.iterrows():
                cur.execute("""INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao)
                               VALUES (?,?,?,?,?,?)""", (r['codigo'], r['descricao'], r['porte'], r['uco'], r['filme'], r['versao']))
            
            registrar_arquivo(h, versao)
            st.success(f"Sucesso: {arq.name}")
        except Exception as e:
            st.error(f"Erro em {arq.name}: {e}")

    con.commit()
    con.close()
    salvar_banco_github(f"Importação {versao}")

# =====================================================
# CONSULTAS
# =====================================================
def versoes():
    try:
        return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", conn())["versao"].tolist()
    except:
        return []

def buscar_dados(termo, versao, tipo):
    campo = "codigo" if tipo == "Código" else "descricao"
    return pd.read_sql(f"SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE {campo} LIKE ? AND versao = ?", 
                       conn(), params=(f"%{termo}%", versao))

# =====================================================
# INTERFACE STREAMLIT
# =====================================================
baixar_banco()
criar_tabelas()

st.set_page_config(page_title="CBHPM Gestão", layout="wide")
st.title("CBHPM • Gestão Inteligente")

# Seleção de Versão na Sidebar para evitar duplicação e facilitar uso
lista_versoes = versoes()
v_selecionada = st.sidebar.selectbox("Tabela CBHPM Ativa", lista_versoes, key="v_global") if lista_versoes else None

abas = st.tabs(["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar"])

# 1. IMPORTAR
with abas[0]:
    v_imp = st.text_input("Nome da Versão (ex: CBHPM 2024)")
    arqs = st.file_uploader("Upload arquivos", accept_multiple_files=True)
    if st.button("Executar Importação", key="btn_imp"):
        importar(arqs, v_imp)
        st.rerun()

# 2. CONSULTAR
with abas[1]:
    if v_selecionada:
        st.info(f"Pesquisando na: **{v_selecionada}**")
        col1, col2 = st.columns([1, 3])
        tipo = col1.radio("Buscar por", ["Código", "Descrição"], key="tipo_busca")
        termo = col2.text_input("Termo de busca", key="termo_busca")
        if st.button("Buscar", key="btn_busca"):
            st.dataframe(buscar_dados(termo, v_selecionada, tipo), use_container_width=True)
    else:
        st.warning("Nenhuma versão disponível. Importe dados primeiro.")

# 3. CALCULAR
with abas[2]:
    if v_selecionada:
        cod_calc = st.text_input("Código do procedimento", key="cod_calc")
        c1, c2, c3 = st.columns(3)
        v_uco = c1.number_input("Valor UCO", value=1.0)
        v_filme = c2.number_input("Valor Filme", value=21.70)
        infla = c3.number_input("Acréscimo %", value=0.0)
        
        if st.button("Calcular", key="btn_calc"):
            res = buscar_dados(cod_calc, v_selecionada, "Código")
            if not res.empty:
                p = res.iloc[0]
                f = 1 + (infla/100)
                tot = (p['porte']*f) + (p['uco']*v_uco*f) + (p['filme']*v_filme*f)
                st.metric(f"Total - {p['descricao']}", f"R$ {tot:,.2f}")
            else: st.error("Código não encontrado.")

# 4. COMPARAR
with abas[3]:
    if len(lista_versoes) >= 2:
        col_a, col_b = st.columns(2)
        va = col_a.selectbox("Versão Base", lista_versoes, key="va")
        vb = col_b.selectbox("Versão Comparação", lista_versoes, key="vb")
        if st.button("Comparar", key="btn_comp"):
            dfa = buscar_dados("", va, "Código")
            dfb = buscar_dados("", vb, "Código").rename(columns={"porte":"porte_B","uco":"uco_B","filme":"filme_B"})
            st.dataframe(dfa.merge(dfb, on="codigo"), use_container_width=True)
    else: st.info("Necessário ao menos 2 versões para comparar.")

# 5. EXPORTAR
with abas[4]:
    if st.button("Gerar Excel Completo", key="btn_exp"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            pd.read_sql("SELECT * FROM procedimentos", conn()).to_excel(writer, sheet_name="Dados", index=False)
        st.download_button("Baixar Arquivo", output.getvalue(), "cbhpm_full.xlsx")

# 6. EXCLUIR
with abas[5]:
    v_excluir = st.selectbox("Versão para deletar", lista_versoes, key="v_del")
    confirma = st.checkbox("Confirmar exclusão irreversível")
    if st.button("Deletar Versão", key="btn_del"):
        if confirma:
            n = excluir_versao(v_excluir)
            st.success(f"Removidos {n} itens.")
            st.rerun()
        else: st.warning("Marque a confirmação.")
