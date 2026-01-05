from fastapi import FastAPI
import sqlite3

app = FastAPI(title="CBHPM API")

def conn():
    return sqlite3.connect("cbhpm_database.db")

@app.get("/versoes")
def versoes():
    c = conn()
    r = c.execute("SELECT DISTINCT versao FROM procedimentos").fetchall()
    c.close()
    return [x[0] for x in r]

@app.get("/procedimento")
def procedimento(codigo:str, versao:str):
    c = conn()
    r = c.execute("""
        SELECT codigo,descricao,porte,uco,filme
        FROM procedimentos
        WHERE codigo = ? AND versao = ?
    """,(codigo,versao)).fetchone()
    c.close()

    if not r:
        return {"erro":"não encontrado"}

    return dict(zip(
        ["codigo","descricao","porte","uco","filme"], r
    ))
