import pandas as pd
import sqlite3
import os

def criar_banco_online():
    # Nome do banco de dados que será gerado
    conn = sqlite3.connect('cbhpm.db')
    
    # Lista exata das versões conforme os seus arquivos
    versoes = [
        "CBHPM 3", "CBHPM 4", "CBHPM 5 (2008)", "CBHPM 5 (2009)", 
        "CBHPM 2010", "CBHPM 2012", "CBHPM 2014", "CBHPM 2016", 
        "CBHPM 2018", "CBHPM 2020", "CBHPM 2022"
    ]

    for v in versoes:
        # Padronizando o nome do arquivo conforme o seu upload
        arquivo = f"CBHPM - Várias Versões.xlsx - {v}.csv"
        
        if os.path.exists(arquivo):
            try:
                # Lendo o CSV
                df = pd.read_csv(arquivo)
                
                # Adicionando a coluna de versão
                df['versao_tabela'] = v
                
                # Padronizando nomes de colunas (Removendo espaços e caracteres estranhos)
                df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]
                
                # Salva na tabela 'procedimentos' dentro do .db
                df.to_sql('procedimentos', conn, if_exists='append', index=False)
                print(f"Sucesso: {v} adicionado.")
            except Exception as e:
                print(f"Erro ao processar {v}: {e}")
        else:
            print(f"Arquivo não encontrado: {arquivo}")

    conn.close()
    print("Arquivo cbhpm.db gerado com sucesso!")

if __name__ == "__main__":
    criar_banco_online()
