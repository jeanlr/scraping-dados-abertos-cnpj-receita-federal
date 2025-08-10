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
