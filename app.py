import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

DB_NAME = "cbhpm_database.db"

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
    CREATE TABLE IF NOT EXISTS convenios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT UNIQUE,
        inflator REAL,
        valor_filme REAL
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

        except Exception as e:
            cur.execute("""
                INSERT INTO log_importacao
                (versao, arquivo, problema, data)
                VALUES (?,?,?,?)
            """, (versao, arq.name, str(e), datetime.now().isoformat()))

    con.commit()
    con.close()

# =====================================================
# CONSULTAS
# =====================================================
def versoes():
    return pd.read_sql(
        "SELECT DISTINCT versao FROM procedimentos ORDER BY versao",
        conn()
    )["versao"].tolist()

def buscar_codigo(codigo, versao):
    return pd.read_sql("""
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE codigo LIKE ? AND versao = ?
    """, conn(), params=(f"%{codigo}%", versao))

def buscar_descricao(desc, versao):
    return pd.read_sql("""
        SELECT codigo, descricao, porte, uco, filme
        FROM procedimentos
        WHERE descricao LIKE ? AND versao = ?
    """, conn(), params=(f"%{desc}%", vers
