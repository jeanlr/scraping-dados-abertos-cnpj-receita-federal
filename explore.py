# %%
# Abre conexão com o arquivo DuckDB
import duckdb


con = duckdb.connect("dados.duckdb")

# Exemplo: listar tabelas existentes
tables = con.execute("SHOW TABLES").fetchall()
print("Tabelas:", tables)

# %%
# Exemplo: ler dados da tabela CNPJ_EMPRESAS
df = con.execute("SELECT SITUACAO_CADASTRAL, count(SITUACAO_CADASTRAL) FROM CNPJ_EMPRESAS GROUP BY ALL").fetchdf()
print(df)

con.close()
# %%
# Exemplo: ler dados da tabela CNPJ_EMPRESAS
total_rows = con.execute("SELECT COUNT(*) FROM CNPJ_EMPRESAS").fetchone()[0]
print(f"Total de linhas na CNPJ_EMPRESAS: {total_rows:,}")

con.close()


# %%
##### Exemplo de LEFT JOIN com DuckDB e pandas para combinar dados de uma lista de CNPJs com a tabela CNPJ_EMPRESAS
import duckdb
import pandas as pd

# Conexão com o banco
con = duckdb.connect("dados.duckdb")

# Lê o Excel usando pandas
excel_path = "lista_cnpjs.xlsx"
df_lista = pd.read_excel(excel_path)

# Registra o DataFrame como tabela temporária no DuckDB
con.register("ListaCNPJs", df_lista)

# Faz o LEFT JOIN
resultado = con.execute("""
    SELECT
        l.CNPJ,
        c.*
    FROM ListaCNPJs l
    LEFT JOIN CNPJ_EMPRESAS c
        ON l.CNPJ = c.CNPJ
""").fetchdf()

# Salva resultado em CSV
resultado.to_csv("resultado_join.csv", index=False, encoding="latin1")
# %%
