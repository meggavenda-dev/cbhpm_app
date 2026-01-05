import os
import base64
import hashlib
import time
import sqlite3
from io import BytesIO
from contextlib import contextmanager
from datetime import datetime
import csv

import pandas as pd
import requests
import altair as alt
import streamlit as st

# =====================================================
# CONFIGURAÇÕES E ESTADO DA SESSÃO
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

DEBUG = bool(st.secrets.get("DEBUG", False))
UCO_DEFAULT = float(st.secrets.get("UCO_VALOR", 1.00))

# Estados iniciais
if 'comparacao_realizada' not in st.session_state:
    st.session_state.comparacao_realizada = False

# Aba preferida (controla índice do radio). Começa em "📋 Consultar".
if "aba_pref" not in st.session_state:
    st.session_state.aba_pref = "📋 Consultar"

# =====================================================
# AJUDANTES
# =====================================================
def warn_user(msg, exc=None):
    """Mostra warning; se DEBUG=True, exibe exceção detalhada."""
    if DEBUG and exc is not None:
        st.exception(exc)
    else:
        st.warning(msg)

def sanitize_str(x):
    return str(x).strip()

# =====================================================
# CONEXÃO E BANCO DE DADOS
# =====================================================
@st.cache_resource
def get_connection():
    # check_same_thread=False é essencial para Streamlit (multithread)
    con = sqlite3.connect(DB_NAME, check_same_thread=False, timeout=30)
    cur = con.cursor()
    # PRAGMAs: melhor desempenho e integridade
    cur.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;
    """)
    return con

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
                codigo TEXT NOT NULL,
                descricao TEXT NOT NULL,
                porte REAL NOT NULL DEFAULT 0,
                uco REAL NOT NULL DEFAULT 0,
                filme REAL NOT NULL DEFAULT 0,
                versao TEXT NOT NULL,
                UNIQUE (codigo, versao)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_proc_cod ON procedimentos (codigo)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_proc_ver ON procedimentos (versao)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_proc_desc ON procedimentos (descricao)")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS arquivos_importados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                versao TEXT NOT NULL,
                data TEXT NOT NULL
            )
        """)

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
    except:
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

def read_csv_smart(file):
    """Detecta delimitador e encoding de forma simples para CSV."""
    file.seek(0)
    sample_bytes = file.read(2048)
    # tentar utf-8, senão latin-1
    try:
        sample_str = sample_bytes.decode("utf-8")
        enc = "utf-8"
    except UnicodeDecodeError:
        sample_str = sample_bytes.decode("latin-1", errors="ignore")
        enc = "latin-1"
    # detectar delimitador
    try:
        dialect = csv.Sniffer().sniff(sample_str, delimiters=[",", ";", "\t", "|"])
        sep = dialect.delimiter
    except Exception:
        sep = ";"
    file.seek(0)
    return pd.read_csv(file, sep=sep, encoding=enc)

# =====================================================
# GITHUB – PERSISTÊNCIA
# =====================================================
def baixar_banco():
    # Se já existe local, não baixa
    if os.path.exists(DB_NAME):
        return
    try:
        repo = st.secrets.get('GITHUB_REPO')
        token = st.secrets.get('GITHUB_TOKEN')
        branch = st.secrets.get('GITHUB_BRANCH', 'main')
        if not repo or not token:
            # Cria um DB vazio caso não tenha secrets
            open(DB_NAME, "wb").close()
            return

        url = f"https://api.github.com/repos/{repo}/contents/{DB_NAME}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        r = requests.get(url, headers=headers, params={"ref": branch})
        if r.status_code == 200:
            content = r.json().get("content")
            if content:
                with open(DB_NAME, "wb") as f:
                    f.write(base64.b64decode(content))
            else:
                open(DB_NAME, "wb").close()
        else:
            open(DB_NAME, "wb").close()
    except Exception as e:
        warn_user("Falha ao baixar banco do GitHub. Criando DB local vazio.", e)
        open(DB_NAME, "wb").close()

def salvar_banco_github(msg, retry=1):
    """Envia o arquivo SQLite para o repositório. Faz retry simples em caso de sha defasado."""
    try:
        repo = st.secrets.get('GITHUB_REPO')
        token = st.secrets.get('GITHUB_TOKEN')
        branch = st.secrets.get('GITHUB_BRANCH', 'main')
        if not repo or not token:
            warn_user("Sincronização com GitHub indisponível (verifique secrets).")
            return

        # Aviso de tamanho grande
        size_mb = os.path.getsize(DB_NAME) / (1024 * 1024)
        if size_mb > 90:
            warn_user(f"Arquivo do banco com {size_mb:.1f} MB. Commits grandes podem falhar no GitHub.")

        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()

        api_url = f"https://api.github.com/repos/{repo}/contents/{DB_NAME}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }

        # Obtém sha atual (se existir)
        r = requests.get(api_url, headers=headers, params={"ref": branch})
        sha = r.json().get("sha") if r.status_code == 200 else None
        payload = {"message": msg, "content": content, "branch": branch}
        if sha:
            payload["sha"] = sha

        put_r = requests.put(api_url, headers=headers, json=payload)
        if put_r.status_code in (200, 201):
            return
        # Retry simples: refaz GET e PUT uma vez
        if retry > 0:
            time.sleep(0.8)
            r2 = requests.get(api_url, headers=headers, params={"ref": branch})
            sha2 = r2.json().get("sha") if r2.status_code == 200 else None
            if sha2:
                payload["sha"] = sha2
            put_r2 = requests.put(api_url, headers=headers, json=payload)
            if put_r2.status_code in (200, 201):
                return
        warn_user(f"Erro na sincronização com GitHub (status {put_r.status_code}).")
    except Exception as e:
        warn_user("Erro na sincronização com GitHub.", e)

# =====================================================
# LÓGICA DE NEGÓCIO
# =====================================================
def importar(arquivos, versao):
    if not versao:
        st.error("Informe a Versão CBHPM.")
        return False

    mapa = {
        "codigo": ["Código", "Codigo"],
        "descricao": ["Descrição", "Descricao"],
        "porte": ["Porte"],
        "uco": ["UCO", "CH"],
        "filme": ["Filme"]
    }

    arquivos_processados = 0
    prog = st.progress(0, text="Preparando importação...")
    total_arqs = max(len(arquivos), 1)

    with gerenciar_db() as con:
        cur = con.cursor()

        for idx, arq in enumerate(arquivos, start=1):
            try:
                h = gerar_hash_arquivo(arq)
                cur.execute("SELECT 1 FROM arquivos_importados WHERE hash=?", (h,))
                if cur.fetchone():
                    st.warning(f"O arquivo '{arq.name}' já foi importado anteriormente.")
                    prog.progress(min(idx / total_arqs, 1.0), text=f"Arquivo {idx}/{total_arqs} (já importado)")
                    continue

                # Ler CSV/Excel de forma robusta
                try:
                    if arq.name.lower().endswith(".csv"):
                        df = read_csv_smart(arq)
                    else:
                        df = pd.read_excel(arq)
                except Exception as e:
                    st.error(f"Erro ao ler {arq.name}: {e}")
                    prog.progress(min(idx / total_arqs, 1.0), text=f"Arquivo {idx}/{total_arqs} (erro de leitura)")
                    continue

                df.columns = [c.strip() for c in df.columns]

                # Valida presença de colunas de código e descrição
                cod_col = next((c for c in mapa["codigo"] if c in df.columns), None)
                desc_col = next((c for c in mapa["descricao"] if c in df.columns), None)
                if cod_col is None or desc_col is None:
                    st.error(f"Arquivo {arq.name} não contém colunas de Código/Descrição esperadas.")
                    prog.progress(min(idx / total_arqs, 1.0), text=f"Arquivo {idx}/{total_arqs} (colunas inválidas)")
                    continue

                dados_lista = []
                for _, row in df.iterrows():
                    d = {campo: extrair_valor(row, df, cols) for campo, cols in mapa.items()}
                    cod = sanitize_str(row[cod_col])
                    desc = sanitize_str(row[desc_col])
                    if not cod or not desc:
                        continue  # pula linhas inválidas
                    dados_lista.append((cod, desc, d["porte"], d["uco"], d["filme"], versao))

                # Inserts em chunks para grandes volumes
                SQL_INSERT = """
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                CHUNK = 5000
                for i in range(0, len(dados_lista), CHUNK):
                    cur.executemany(SQL_INSERT, dados_lista[i:i+CHUNK])

                cur.execute("""
                    INSERT OR IGNORE INTO arquivos_importados (hash, versao, data)
                    VALUES (?, ?, ?)
                """, (h, versao, datetime.now().isoformat()))
                arquivos_processados += 1
                prog.progress(min(idx / total_arqs, 1.0), text=f"Arquivo {idx}/{total_arqs} importado")
            except Exception as e:
                warn_user(f"Falha ao importar '{getattr(arq, 'name', 'arquivo')}'.", e)
                prog.progress(min(idx / total_arqs, 1.0), text=f"Arquivo {idx}/{total_arqs} (falha)")

    if arquivos_processados > 0:
        salvar_banco_github(f"Importação {versao} — {arquivos_processados} arquivo(s)")
        return True
    return False

@st.cache_data(ttl=300)
def versoes():
    with get_connection() as con:
        try:
            return pd.read_sql("SELECT DISTINCT versao FROM procedimentos ORDER BY versao", con)["versao"].tolist()
        except Exception:
            return []

def buscar_dados(termo, versao, tipo):
    campo = "codigo" if tipo == "Código" else "descricao"
    with get_connection() as con:
        return pd.read_sql(
            f"""
            SELECT codigo, descricao, porte, uco, filme
            FROM procedimentos
            WHERE {campo} LIKE ? AND versao = ?
            ORDER BY codigo
            """,
            con, params=(f"%{termo}%", versao)
        )

def show_dataframe_paginated(df, page_size=200):
    total = len(df)
    if total == 0:
        st.info("Nenhum registro para exibir.")
        return
    num_pages = max(1, (total - 1) // page_size + 1)
    cols = st.columns([1, 1, 2])
    with cols[0]:
        st.caption(f"Total: {total} • Páginas: {num_pages}")
    with cols[1]:
        page = st.number_input("Página", min_value=1, max_value=num_pages, value=1, step=1)
    s = (page - 1) * page_size
    e = s + page_size
    st.dataframe(df.iloc[s:e], use_container_width=True, hide_index=True)

# =====================================================
# INICIALIZAÇÃO
# =====================================================
baixar_banco()
criar_tabelas()

st.set_page_config(page_title="CBHPM Gestão Inteligente", layout="wide")
st.title("⚖️ CBHPM • Auditoria e Gestão")

# =====================================================
# TEMA GLOBAL (CSS)
# =====================================================
st.markdown("""
<style>
:root{
  --primary:#1E88E5; --primary-700:#1b78ca;
  --success:#10B981; --warning:#F59E0B; --error:#EF4444;
  --text:#111827; --muted:#6B7280; --bg:#F7FAFC; --white:#ffffff; --border:#E5E7EB;
}
html, body, [data-testid="stAppViewContainer"]{
  background: var(--bg); color: var(--text);
  font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, 'Helvetica Neue', Arial, 'Noto Sans', sans-serif;
}
h1, h2, h3 { letter-spacing: .2px; }
h1 { font-weight: 800; }
h2 { font-weight: 700; }
.card {
  background: var(--white); border-radius: 12px; padding: 18px; border: 1px solid var(--border);
  box-shadow: 0 2px 10px rgba(17,24,39,.06); margin-bottom: 16px;
}
.res-card {
  padding: 16px 18px; border-radius: 12px; background-color: var(--white);
  border-left: 6px solid var(--primary); box-shadow: 0 2px 10px rgba(17,24,39,.06); margin: 8px 0 18px 0;
}
.res-card small { color: var(--muted); }
[data-testid="stMetricValue"] { font-size: 1.9rem; color: var(--primary); font-weight: 800; }
[data-testid="stMetricLabel"] { color: var(--muted); font-weight: 600; letter-spacing: .2px; }
.stButton>button {
  background: var(--primary)!important; color: #fff!important; border-radius: 10px!important;
  border: 1px solid var(--primary-700)!important; padding: .55rem .9rem!important;
}
.stButton>button:hover { filter: brightness(1.06); }
.stTextInput input, .stNumberInput input, .stSelectbox div[data-baseweb="select"]>div {
  border-radius: 10px; border: 1px solid var(--border);
}
.stRadio [role="radiogroup"] { gap: .5rem; }
[data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid var(--border); }
.total-strip {
  display:flex; align-items:center; justify-content:space-between; padding: 12px 16px; border-radius: 10px;
  background: #ffffff; border:1px solid var(--border); box-shadow: 0 2px 10px rgba(17,24,39,.05);
}
.total-strip .label { font-weight:700; color:var(--text); letter-spacing:.2px; }
.total-strip .value { font-size:1.6rem; color:var(--success); font-weight:800; }
.badge {
  display:inline-block; padding:4px 8px; border-radius:999px; font-size:.75rem;
  border:1px solid var(--border); background:#fff; color:var(--muted); margin-right:6px;
}
.stDataFrame { border-radius: 10px; overflow: hidden; }
small.note { color: var(--muted); font-size: .85rem; }
</style>
""", unsafe_allow_html=True)

# =====================================================
# NAVEGAÇÃO (Sidebar)
# =====================================================
opcoes = ["📋 Consultar", "🧮 Calcular", "⚖️ Comparar", "📤 Exportar", "🗑️ Gerenciar", "📥 Importar"]
aba_atual = st.sidebar.radio(
    "Navegação",
    opcoes,
    index=opcoes.index(st.session_state.get("aba_pref", "📋 Consultar"))
)

# =====================================================
# 1) IMPORTAR
# =====================================================
if aba_atual == "📥 Importar":
    st.subheader("📥 Carregar Novos Dados")
    st.caption("Faça upload de arquivos CSV/Excel com procedimentos CBHPM.")

    # Variáveis de controle no estado da sessão
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
            st.markdown('<div class="card">', unsafe_allow_html=True)
            with st.form("form_importacao", clear_on_submit=True):
                v_imp_input = st.text_input("Nome da Versão (ex: CBHPM 2024)")
                arqs_input = st.file_uploader("Upload arquivos (CSV ou Excel)", accept_multiple_files=True)
                submitted = st.form_submit_button("🚀 Iniciar Importação Agora")
            st.markdown('</div>', unsafe_allow_html=True)
            if submitted:
                if not v_imp_input or not arqs_input:
                    st.error("Preencha o nome da versão e selecione os arquivos.")
                else:
                    st.session_state.temp_v_imp = v_imp_input
                    st.session_state.temp_arqs = arqs_input
                    st.session_state.processando = True
                    st.rerun()  # Reinicia para trocar a tela
    else:
        # EXIBE O STATUS DE PROCESSAMENTO (O formulário sumiu)
        with area_dinamica.container():
            st.info(f"⚙️ Processando: **{st.session_state.temp_v_imp}**")
            if importar(st.session_state.temp_arqs, st.session_state.temp_v_imp):
                st.toast("Dados processados com sucesso!", icon="✅")
                st.success("✅ Importação concluída! O sistema será atualizado.")
                st.cache_data.clear()
                st.session_state.processando = False
                st.session_state.temp_arqs = None
                st.session_state.temp_v_imp = ""
                st.session_state.aba_pref = "📋 Consultar"
                time.sleep(1)
                st.rerun()
            else:
                st.error("Erro crítico na importação.")
                if st.button("Tentar Novamente"):
                    st.session_state.processando = False
                    st.rerun()

# =====================================================
# 2) CONSULTAR  (COM BOTÃO DE PESQUISA + PAGINAÇÃO)
# =====================================================
if aba_atual == "📋 Consultar":
    lista_v = versoes()
    v_selecionada = st.sidebar.selectbox("Tabela Ativa", lista_v, key="v_global_consulta") if lista_v else None

    if v_selecionada:
        st.subheader("📋 Consulta de Procedimentos")
        st.caption("Preencha os campos e clique em **🔎 Pesquisar**. Você pode paginar e baixar os resultados.")

        st.markdown('<div class="card">', unsafe_allow_html=True)
        with st.form("form_consulta"):
            c1, c2 = st.columns([1, 3])
            tipo = c1.radio("Busca por", ["Código", "Descrição"], horizontal=True, help="Escolha por código ou descrição.")
            termo = c2.text_input("Digite o termo de busca...", help="Ex.: '10101012' ou parte da descrição.")
            pesquisar = st.form_submit_button("🔎 Pesquisar")
        st.markdown('</div>', unsafe_allow_html=True)

        if pesquisar:
            if termo.strip() == "":
                st.warning("Digite um termo de busca antes de pesquisar.")
            else:
                res = buscar_dados(termo, v_selecionada, tipo)
                if res.empty:
                    st.info("Nenhum resultado encontrado para o termo informado.")
                else:
                    show_dataframe_paginated(res, page_size=200)
                    csv_data = res.to_csv(index=False).encode("utf-8")
                    st.download_button("📥 Baixar resultados (CSV)", csv_data, "resultados_consulta.csv", "text/csv")
        else:
            st.caption("Preencha e clique em **🔎 Pesquisar**.")
    else:
        st.warning("Nenhuma versão disponível. Importe dados na aba '📥 Importar'.")

# =====================================================
# 3) CALCULAR  (UCO automático; mantém métrica UCO; checkboxes reativos + design)
# =====================================================
if aba_atual == "🧮 Calcular":
    lista_v = versoes()
    v_selecionada = st.sidebar.selectbox("Tabela Ativa", lista_v, key="v_global_calc") if lista_v else None

    if v_selecionada:
        st.subheader("🧮 Calculadora de Honorários CBHPM")
        st.caption("Preencha os campos. O cálculo atualiza automaticamente conforme você marca os componentes a ajustar.")

        UCO_VALOR_APLICADO = float(st.secrets.get("UCO_VALOR", UCO_DEFAULT))

        # Grupo de entradas em card
        st.markdown('<div class="card">', unsafe_allow_html=True)
        col_cod, col_ajuste, col_filme = st.columns([2, 1, 1.2])
        cod_calc = col_cod.text_input("Código do Procedimento", placeholder="Ex: 10101012",
                                      key="in_calc", help="Código conforme a versão ativa.")
        infla = col_ajuste.number_input("Ajuste Adicional (%)", 0.0, step=0.5,
                                        key="in_infla", help="Percentual do ajuste.")
        filme_v = col_filme.number_input("Valor Filme (R$)", 21.70, step=0.01, format="%.2f",
                                         key="in_filme_val", help="Valor unitário de filme.")

        st.write("**Aplicar ajuste em:** (marque para incluir no ajuste)")
        c_port, c_uco, c_fil = st.columns(3)
        aplicar_porte = c_port.checkbox("Porte", value=False, key="chk_aplicar_porte")
        aplicar_uco   = c_uco.checkbox("UCO",   value=False, key="chk_aplicar_uco")
        aplicar_filme = c_fil.checkbox("Filme", value=False, key="chk_aplicar_filme")
        st.markdown('</div>', unsafe_allow_html=True)

        # Cálculo reativo
        if not cod_calc:
            st.info("Informe o **Código do Procedimento** para calcular.")
        else:
            res = buscar_dados(cod_calc, v_selecionada, "Código")
            if res.empty:
                st.error(f"O código '{cod_calc}' não foi encontrado na tabela {v_selecionada}.")
            else:
                p = res.iloc[0]
                f_porte = (1 + infla/100) if (aplicar_porte and infla != 0) else 1.0
                f_uco   = (1 + infla/100) if (aplicar_uco   and infla != 0) else 1.0
                f_filme = (1 + infla/100) if (aplicar_filme and infla != 0) else 1.0

                porte_calc = p['porte'] * f_porte
                uco_calc   = p['uco']   * UCO_VALOR_APLICADO * f_uco
                filme_calc = p['filme'] * filme_v * f_filme
                total = porte_calc + uco_calc + filme_calc

                # Card do procedimento
                st.markdown(f"""
                    <div class="res-card">
                        <small>Versão ativa: <b>{v_selecionada}</b></small><br>
                        <span style='font-size: 1.05rem; font-weight: 600;'>{p['descricao']}</span><br>
                        <small class="note">Código {cod_calc}</small>
                    </div>
                """, unsafe_allow_html=True)

                # Métricas
                c_porte, c_uco_box, c_filme = st.columns(3)
                c_porte.metric("Porte", f"R$ {porte_calc:,.2f}")
                c_uco_box.metric("UCO", f"R$ {uco_calc:,.2f}")
                c_filme.metric("Filme", f"R$ {filme_calc:,.2f}")

                # Faixa TOTAL
                houve_ajuste = (infla != 0) and (aplicar_porte or aplicar_uco or aplicar_filme)
                st.markdown(f"""
                    <div class="total-strip" style="border-left:6px solid var(--success); margin-top: 4px;">
                        <div class="label">TOTAL FINAL</div>
                        <div class="value">R$ {total:,.2f}</div>
                    </div>
                """, unsafe_allow_html=True)

                # Badges de escopo
                comps = []
                if aplicar_porte: comps.append("Porte")
                if aplicar_uco:   comps.append("UCO")
                if aplicar_filme: comps.append("Filme")
                if houve_ajuste:
                    st.markdown("".join([f"<span class='badge'>{c}</span>" for c in comps]), unsafe_allow_html=True)
                    st.caption(f"🧮 Ajuste de {infla:.2f}% aplicado nos componentes marcados.")
                else:
                    st.caption("🔧 Nenhum componente marcado para ajuste ou ajuste adicional está zero.")

                st.divider()
    else:
        st.warning("Nenhuma versão disponível. Importe dados na aba '📥 Importar'.")

# =====================================================
# 4) COMPARAR  (tratamento porte==0, média+mediana)
# =====================================================
if aba_atual == "⚖️ Comparar":
    lista_v = versoes()
    if len(lista_v) >= 2:
        st.subheader("⚖️ Comparação entre Versões")
        st.caption("Selecione versões e analise variações de porte (média e mediana).")

        col1, col2 = st.columns(2)
        v1 = col1.selectbox("Versão Anterior", lista_v, key="v1")
        v2 = col2.selectbox("Versão Atual", lista_v, key="v2")
        
        if st.button("Analisar Reajustes"):
            st.session_state.comparacao_realizada = True
        
        if st.session_state.comparacao_realizada:
            df1 = buscar_dados("", v1, "Código")
            df2 = buscar_dados("", v2, "Código").rename(
                columns={"porte":"porte_2", "uco":"uco_2", "filme":"filme_2", "descricao":"desc_2"}
            )
            comp = df1.merge(df2, on="codigo")
            
            if not comp.empty:
                base = comp['porte']
                comp['var_porte'] = ((comp['porte_2'] - base) / base.replace(0, pd.NA)) * 100

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Itens Comuns", len(comp))
                m2.metric("Média var. porte", f"{comp['var_porte'].dropna().mean():.2f}%")
                m3.metric("Mediana var. porte", f"{comp['var_porte'].dropna().median():.2f}%")
                m4.metric("Porte=0 (base)", int((base == 0).sum()))

                resumo = comp.groupby(comp['codigo'].str[:2], dropna=False)['var_porte'].mean().reset_index()
                chart = alt.Chart(resumo).mark_bar().encode(
                    x=alt.X('codigo:N', title="Grupo (Capítulo)"),
                    y=alt.Y('var_porte:Q', title="Variação % (média)"),
                    color=alt.condition(alt.datum.var_porte > 0, alt.value('#1E88E5'), alt.value('#F59E0B')),
                    tooltip=[alt.Tooltip('codigo:N', title='Grupo'), alt.Tooltip('var_porte:Q', title='Variação média', format='.2f')]
                ).properties(height=320)
                st.altair_chart(chart, use_container_width=True)

                st.dataframe(
                    comp[['codigo', 'descricao', 'porte', 'porte_2', 'var_porte']], 
                    use_container_width=True, hide_index=True,
                    column_config={"var_porte": st.column_config.NumberColumn("Variação %", format="%.2f%%")}
                )
            else:
                st.info("Nenhum item comum entre as versões selecionadas.")
    else:
        st.warning("Necessário ao menos 2 versões para comparar. Importe mais dados na aba '📥 Importar'.")

# =====================================================
# 5) EXPORTAR  (autoajuste colunas)
# =====================================================
if aba_atual == "📤 Exportar":
    lista_v = versoes()
    if lista_v:
        st.subheader("📤 Exportação de Dados")
        st.caption("Gere um backup completo da base (procedimentos e arquivos importados).")
        st.markdown('<div class="card">', unsafe_allow_html=True)
        if st.button("📦 Gerar Backup Completo (Excel)"):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                with get_connection() as con:
                    df1 = pd.read_sql("SELECT * FROM procedimentos", con)
                    df2 = pd.read_sql("SELECT * FROM arquivos_importados", con)

                df1.to_excel(writer, index=False, sheet_name="procedimentos")
                df2.to_excel(writer, index=False, sheet_name="arquivos_importados")

                # autoajuste de colunas
                for sheet_name, df in [("procedimentos", df1), ("arquivos_importados", df2)]:
                    ws = writer.sheets[sheet_name]
                    for i, col in enumerate(df.columns):
                        try:
                            max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                        except Exception:
                            max_len = len(col) + 2
                        ws.set_column(i, i, min(max_len, 40))

            st.download_button("📥 Baixar Arquivo", output.getvalue(), "cbhpm_completa.xlsx")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.warning("Nenhuma versão disponível para exportar. Importe dados na aba '📥 Importar'.")

# =====================================================
# 6) GERENCIAR
# =====================================================
if aba_atual == "🗑️ Gerenciar":
    lista_v = versoes()
    if lista_v:
        st.subheader("🗑️ Gerenciar Versões")
        st.caption("Exclua versões completas. Esta ação é definitiva e sincroniza com o GitHub.")
        st.markdown('<div class="card">', unsafe_allow_html=True)

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
                # Após remoção, forçamos voltar para Consultar
                st.session_state.aba_pref = "📋 Consultar"
                st.rerun()
            else:
                st.info("Marque a confirmação para prosseguir.")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.warning("Nenhuma versão disponível para gerenciar. Importe dados na aba '📥 Importar'.")
