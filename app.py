import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os
import requests
import base64
import hashlib

# =====================================================
# CONFIGURAÇÕES INICIAIS
# =====================================================
DB_NAME = "data/cbhpm_database.db"
os.makedirs("data", exist_ok=True)

# =====================================================
# FUNÇÕES DE CONEXÃO E UTILITÁRIOS (Definir Primeiro)
# =====================================================
def conn():
    """Cria a conexão com o banco de dados SQLite."""
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def to_float(v):
    try:
        if pd.isna(v):
            return 0.0
        if isinstance(v, str):
            v = v.replace(",", ".").strip()
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
    if os.path.exists(DB_NAME):
        return

    try:
        url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        headers = {
            "Authorization": f"token {st.secrets['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github.v3+json"
        }

        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            content = r.json()["content"]
            with open(DB_NAME, "wb") as f:
                f.write(base64.b64decode(content))
        else:
            open(DB_NAME, "wb").close()
    except Exception as e:
        st.error(f"Erro ao baixar banco do GitHub: {e}")

def salvar_banco_github(msg="Atualização automática do banco CBHPM"):
    try:
        with open(DB_NAME, "rb") as f:
            content = base64.b64encode(f.read()).decode()

        api_url = f"https://api.github.com/repos/{st.secrets['GITHUB_REPO']}/contents/{DB_NAME}"
        headers = {
            "Authorization": f"token {st.secrets['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github.v3+json"
        }

        r = requests.get(api_url, headers=headers)
        sha = r.json()["sha"] if r.status_code == 200 else None

        payload = {
            "message": msg,
            "content": content,
            "branch": st.secrets["GITHUB_BRANCH"]
        }

        if sha:
            payload["sha"] = sha

        r2 = requests.put(api_url, headers=headers, json=payload)

        if r2.status_code not in (200, 201):
            st.error(f"Erro ao salvar no GitHub: {r2.status_code} - {r2.text}")
    except Exception as e:
        st.error(f"Falha na sincronização com GitHub: {e}")

# =====================================================
# OPERAÇÕES DE BANCO DE DADOS
# =====================================================
def criar_tabelas():
    con = conn()
    c = con.cursor()

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

    c.execute("""
    CREATE TABLE IF NOT EXISTS arquivos_importados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT UNIQUE,
        versao TEXT,
        data TEXT
    )
    """)

    con.commit()
    con.close()

def arquivo_ja_importado(hash_arquivo):
    con = conn()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM arquivos_importados WHERE hash = ?", (hash_arquivo,))
    existe = cur.fetchone() is not None
    con.close()
    return existe

def registrar_arquivo_importado(hash_arquivo, versao):
    con = conn()
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO arquivos_importados
        (hash, versao, data)
        VALUES (?,?,?)
    """, (hash_arquivo, versao, datetime.now().isoformat()))
    con.commit()
    con.close()

# =====================================================
# IMPORTAÇÃO E LÓGICA DE NEGÓCIO
# =====================================================
def ler_arquivo(arq):
    if arq.name.lower().endswith(".csv"):
        return pd.read_csv(arq, sep=";", encoding="latin-1", engine="python")
    return pd.read_excel(arq)

def importar(arquivos, versao):
    mapa = {
        "codigo": ["Código", "Codigo"],
        "descricao": ["Descrição", "Descricao"],
        "porte": ["Porte", "Porte Cirúrgico"],
        "uco": ["UCO", "CH"],
        "filme": ["Filme", "Filme Rx"]
    }

    con = conn()
    cur = con.cursor()

    for arq in arquivos:
        hash_arquivo = gerar_hash_arquivo(arq)

        if arquivo_ja_importado(hash_arquivo):
            cur.execute("""
                INSERT INTO log_importacao
                (versao, arquivo, problema, data)
                VALUES (?,?,?,?)
            """, (versao, arq.name, "Arquivo já importado anteriormente (hash duplicado)", datetime.now().isoformat()))
            continue

        try:
            df = ler_arquivo(arq)
            df.columns = [c.strip() for c in df.columns]

            dados = {}
            faltando = []

            for campo, colunas in mapa.items():
                col = next((c for c in colunas if c in df.columns), None)
                if col:
                    dados[campo] = df[col]
                else:
                    dados[campo] = 0.0
                    faltando.append(campo)

            if faltando:
                cur.execute("""
                    INSERT INTO log_importacao
                    (versao, arquivo, problema, data)
                    VALUES (?,?,?,?)
                """, (versao, arq.name, f"Colunas ausentes: {faltando}", datetime.now().isoformat()))

            df_f = pd.DataFrame(dados)
            df_f["versao"] = versao

            for c in ["porte", "uco", "filme"]:
                df_f[c] = df_f[c].apply(to_float)

            for _, r in df_f.iterrows():
                cur.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?,?,?,?,?,?)
                """, tuple(r))

            registrar_arquivo_importado(hash_arquivo, versao)

        except Exception as e:
            cur.execute("""
                INSERT INTO log_importacao
                (versao, arquivo, problema, data)
                VALUES (?,?,?,?)
            """, (versao, arq.name, str(e), datetime.now().isoformat()))

    con.commit()
    con.close()
    salvar_banco_github(f"Importação CBHPM {versao}")

# =====================================================
# EXECUÇÃO PRINCIPAL (Sempre ao final)
# =====================================================
# Primeiro inicializamos o banco
baixar_banco()
criar_tabelas()

# Agora a interface do Streamlit
st.title("Sistema de Gestão CBHPM")
st.write("Banco de dados carregado e tabelas verificadas.")

# =====================================================
# BANCO
# =====================================================
def conn():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

# =====================================================
# UTIL
# =====================================================
def to_float(v):
    try:
        if pd.isna(v):
            return 0.0
        if isinstance(v, str):
            v = v.replace(",", ".").strip()
        return float(v)
    except:
        return 0.0

def gerar_hash_arquivo(uploaded_file):
    uploaded_file.seek(0)
    h = hashlib.sha256(uploaded_file.read()).hexdigest()
    uploaded_file.seek(0)
    return h

def arquivo_ja_importado(hash_arquivo):
    con = conn()
    cur = con.cursor()
    cur.execute("SELECT 1 FROM arquivos_importados WHERE hash = ?", (hash_arquivo,))
    existe = cur.fetchone() is not None
    con.close()
    return existe

def registrar_arquivo_importado(hash_arquivo, versao):
    con = conn()
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO arquivos_importados
        (hash, versao, data)
        VALUES (?,?,?)
    """, (hash_arquivo, versao, datetime.now().isoformat()))
    con.commit()
    con.close()

# =====================================================
# TABELAS
# =====================================================
def criar_tabelas():
    con = conn()
    c = con.cursor()

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

    c.execute("""
    CREATE TABLE IF NOT EXISTS arquivos_importados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT UNIQUE,
        versao TEXT,
        data TEXT
    )
    """)

    con.commit()
    con.close()

# =====================================================
# IMPORTAÇÃO
# =====================================================
def ler_arquivo(arq):
    if arq.name.lower().endswith(".csv"):
        return pd.read_csv(arq, sep=";", encoding="latin-1", engine="python")
    return pd.read_excel(arq)

def importar(arquivos, versao):
    mapa = {
        "codigo": ["Código", "Codigo"],
        "descricao": ["Descrição", "Descricao"],
        "porte": ["Porte", "Porte Cirúrgico"],
        "uco": ["UCO", "CH"],
        "filme": ["Filme", "Filme Rx"]
    }

    con = conn()
    cur = con.cursor()

    for arq in arquivos:
        hash_arquivo = gerar_hash_arquivo(arq)

        if arquivo_ja_importado(hash_arquivo):
            cur.execute("""
                INSERT INTO log_importacao
                (versao, arquivo, problema, data)
                VALUES (?,?,?,?)
            """, (
                versao,
                arq.name,
                "Arquivo já importado anteriormente (hash duplicado)",
                datetime.now().isoformat()
            ))
            continue

        try:
            df = ler_arquivo(arq)
            df.columns = [c.strip() for c in df.columns]

            dados = {}
            faltando = []

            for campo, colunas in mapa.items():
                col = next((c for c in colunas if c in df.columns), None)
                if col:
                    dados[campo] = df[col]
                else:
                    dados[campo] = 0.0
                    faltando.append(campo)

            if faltando:
                cur.execute("""
                    INSERT INTO log_importacao
                    (versao, arquivo, problema, data)
                    VALUES (?,?,?,?)
                """, (versao, arq.name, f"Colunas ausentes: {faltando}", datetime.now().isoformat()))

            df_f = pd.DataFrame(dados)
            df_f["versao"] = versao

            for c in ["porte", "uco", "filme"]:
                df_f[c] = df_f[c].apply(to_float)

            for _, r in df_f.iterrows():
                cur.execute("""
                    INSERT OR IGNORE INTO procedimentos
                    (codigo, descricao, porte, uco, filme, versao)
                    VALUES (?,?,?,?,?,?)
                """, tuple(r))

            registrar_arquivo_importado(hash_arquivo, versao)

        except Exception as e:
            cur.execute("""
                INSERT INTO log_importacao
                (versao, arquivo, problema, data)
                VALUES (?,?,?,?)
            """, (versao, arq.name, str(e), datetime.now().isoformat()))

    con.commit()
    con.close()
    salvar_banco_github(f"Importação CBHPM {versao}")

# =====================================================
# RESTANTE DO CÓDIGO
# (consultas, cálculo, comparação, exportação e interface)
# 👉 permanece exatamente igual ao que você enviou
# =====================================================
