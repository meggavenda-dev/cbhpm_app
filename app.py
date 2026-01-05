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
import altair as alt # Adicionado para os gráficos

# =====================================================
# CONFIGURAÇÕES
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

# =====================================================
# CONEXÃO E LIMPEZA (Context Manager)
# =====================================================
@contextmanager
def gerenciar_db():
    """Gerencia a conexão com o banco garantindo fechamento e commit/rollback automático."""
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
            # Remove pontos de milhar e troca vírgula por ponto
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
# OPERAÇÕES DE BANCO DE DADOS
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
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS arquivos_importados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            versao TEXT,
            data TEXT
        )""")

def arquivo_ja_importado(h):
    with sqlite3.connect(DB_NAME) as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM arquivos_importados WHERE hash=?", (h,))
        return cur.fetchone() is not None

def registrar_arquivo(h, versao):
    with gerenciar_db() as con:
        cur = con.cursor()
        cur.execute("INSERT OR IGNORE INTO arquivos_importados VALUES (NULL,?,?,?)", 
                    (h, versao, datetime.now().isoformat()))

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

    mapa = {"codigo": ["Código", "Codigo"], "descricao": ["Descrição", "Descricao"], 
            "porte": ["Porte"], "uco": ["UCO", "CH"], "filme": ["Filme"]}
    
    arquivos_processados = 0

    with gerenciar_db() as con:
        cur = con.cursor()
        for arq in arquivos:
            h = gerar_hash_arquivo(arq)
            if arquivo_ja_importado(h):
                st.warning(f"O conteúdo de '{arq.name}' já foi importado.")
                continue

            if arq.name.lower().endswith(".csv"):
                try:
                    df = pd.read_csv(arq, sep=";", encoding="utf-8")
                except UnicodeDecodeError:
                    arq.seek(0)
                    df = pd.read_csv(arq, sep=";", encoding="latin-1")
            else:
                df = pd.read_excel(arq)
            
            df.columns = [c.strip() for c in df.columns]
            
            dados_lista = []
            for _, row in df.iterrows():
                d = {}
                for campo, cols in mapa.items():
                    col_encontrada = next((c for c in cols if c in df.columns), None)
                    d[campo] = to_float(row[col_encontrada]) if col_encontrada else 0.0
                
                # Pegar as colunas de texto de forma dinâmica
                cod_col = next((c for c in mapa["codigo"] if c in df.columns), df.columns[0])
                desc_col = next((c for c in mapa["descricao"] if c in df.columns), df.columns[1])

                dados_lista.append((
                    str(row[cod_col]),
                    str(row[desc_col]),
                    d["porte"], d["uco"], d["filme"], versao
                ))

            cur.executemany("""
                INSERT OR IGNORE INTO procedimentos (codigo, descricao, porte, uco, filme, versao)
                VALUES (?, ?, ?, ?, ?, ?)
            """, dados_lista)
            
            cur.execute("INSERT OR IGNORE INTO arquivos_importados (hash, versao, data) VALUES (?, ?, ?)", 
                        (h, versao, datetime.now().isoformat()))
            arquivos_processados += 1

    if arquivos_processados > 0:
        salvar_banco_github(f"Importação {versao}")
        return True
    return False

# =====================================================
# CONSULTAS (Com Cache)
# =====================================================
@st.cache_data
def versoes():
    with sqlite3.connect(DB_NAME) as con:
        try:
            return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except:
            return []

def buscar_dados(termo, versao, tipo):
    campo = "codigo" if tipo == "Código" else "descricao"
    with sqlite3.connect(DB_NAME) as con:
        return pd.read_sql(f"SELECT codigo, descricao, porte, uco, filme FROM procedimentos WHERE {campo} LIKE ? AND versao = ?", 
                           con, params=(f"%{termo}%", versao))

# =====================================================
# INTERFACE STREAMLIT
# =====================================================
baixar_banco()
criar_tabelas()

st.set_page_config(page_title="CBHPM Gestão", layout="wide")
st.title("CBHPM • Gestão Inteligente")

lista_versoes = versoes()
v_selecionada = st.sidebar.selectbox("Tabela CBHPM Ativa", lista_versoes, key="v_global") if lista_versoes else None

abas = st.tabs(["📥 Importar", "📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar"])

# 1. IMPORTAR
with abas[0]:
    v_imp = st.text_input("Nome da Versão (ex: CBHPM 2024)")
    arqs = st.file_uploader("Upload arquivos", accept_multiple_files=True)
    if st.button("Executar Importação", key="btn_imp"):
        if importar(arqs, v_imp):
            st.success(f"Tabela '{v_imp}' importada com sucesso!")
            st.balloons()
            st.cache_data.clear()
            time.sleep(2)
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
        st.warning("Nenhuma versão disponível.")

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

# 4. COMPARAR (Dashboard + Gráfico)
with abas[3]:
    if len(lista_versoes) >= 2:
        col_v1, col_v2 = st.columns(2)
        va = col_v1.selectbox("Versão Base (Antiga)", lista_versoes, key="va")
        vb = col_v2.selectbox("Versão Comparação (Nova)", lista_versoes, key="vb")
        
        if st.button("Analisar Diferenças", key="btn_comp"):
            df_a = buscar_dados("", va, "Código")
            df_b = buscar_dados("", vb, "Código").rename(
                columns={"porte": "porte_B", "uco": "uco_B", "filme": "filme_B", "descricao": "desc_B"}
            )
            comp = df_a.merge(df_b, on="codigo")
            
            if not comp.empty:
                comp['diff_porte'] = comp['porte_B'] - comp['porte']
                # Tratamento para evitar divisão por zero
                comp['perc_var'] = (comp['diff_porte'] / comp['porte'].replace(0, 1)) * 100
                
                st.subheader(f"📊 Dashboard: {va} vs {vb}")
                m1, m2, m3 = st.columns(3)
                m1.metric("Itens Comuns", len(comp))
                m2.metric("Variação Média de Porte", f"{comp['perc_var'].mean():.2f}%")
                m3.metric("Itens com Aumento", len(comp[comp['diff_porte'] > 0]))

                # --- NOVO: GRÁFICO DE BARRAS POR CATEGORIA ---
                st.write("### 📈 Aumento Médio por Grupo de Procedimento")
                comp['Grupo'] = comp['codigo'].astype(str).str[:2]
                resumo_grupo = comp.groupby('Grupo')['perc_var'].mean().reset_index()
                resumo_grupo.columns = ['Grupo', 'Aumento Médio (%)']
                
                grafico = alt.Chart(resumo_grupo).mark_bar().encode(
                    x=alt.X('Grupo:N', sort='-y', title="Grupo (Início do Código)"),
                    y=alt.Y('Aumento Médio (%):Q', title="Variação %"),
                    color=alt.condition(
                        alt.datum['Aumento Médio (%)'] > 0,
                        alt.value('steelblue'), # Azul para aumento
                        alt.value('orange')     # Laranja para redução
                    ),
                    tooltip=['Grupo', 'Aumento Médio (%)']
                ).properties(height=400)
                
                st.altair_chart(grafico, use_container_width=True)

                st.divider()
                st.write("### Tabela Comparativa")
                st.dataframe(comp[['codigo', 'descricao', 'porte', 'porte_B', 'perc_var']], use_container_width=True)
            else:
                st.warning("Sem códigos em comum para comparar.")
    else: st.info("Necessário pelo menos 2 versões para comparar.")

# 5. EXPORTAR
with abas[4]:
    if st.button("Gerar Excel Completo", key="btn_exp"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            with sqlite3.connect(DB_NAME) as con:
                pd.read_sql("SELECT * FROM procedimentos", con).to_excel(writer, sheet_name="Dados", index=False)
        st.download_button("Baixar Arquivo", output.getvalue(), "cbhpm_full.xlsx")

# 6. GERENCIAR
with abas[5]:
    if lista_versoes:
        v_excluir = st.selectbox("Versão para deletar", lista_versoes, key="v_del")
        confirma = st.checkbox("Confirmo a exclusão definitiva")
        if st.button("Deletar Versão", key="btn_del"):
            if confirma:
                n = excluir_versao(v_excluir)
                st.success(f"Versão '{v_excluir}' removida!")
                st.cache_data.clear()
                time.sleep(2)
                st.rerun()
