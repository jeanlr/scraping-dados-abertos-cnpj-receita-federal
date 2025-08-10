# %%
import os
import duckdb
import pandas as pd

# Caminho da pasta raw
PASTA_RAW = "raw"
BANCO_DUCKDB = "dados.duckdb"

# Dicionário de chaves e cabeçalhos
cabecalhos = {
    'EMPRE': [
        "CNPJ_BASICO", "RAZAO_SOCIAL_NOME_EMPRESARIAL", "NATUREZA_JURIDICA",
        "QUALIFICACAO_DO_RESPONSAVEL", "CAPITAL_SOCIAL_DA_EMPRESA",
        "PORTE_DA_EMPRESA", "ENTE_FEDERATIVO_RESPONSAVEL"
    ],
    'ESTABELE': [
        "CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV", "IDENTIFICADOR_MATRIZ_FILIAL",
        "NOME_FANTASIA", "SITUACAO_CADASTRAL", "DATA_SITUACAO", "MOTIVO_SITUACAO_CADASTRAL",
        "NOME_DA_CIDADE_NO_EXTERIOR", "PAIS", "DATA_DE_INICIO_ATIVIDADE", "CNAE_FISCAL_PRINCIPAL",
        "CNAE_FISCAL_SECUNDARIA", "TIPO_DE_LOGRADOURO", "LOGRADOURO", "NUMERO",
        "COMPLEMENTO", "BAIRRO", "CEP", "UF", "MUNICIPIO", "DDD_1", "TELEFONE_1",
        "DDD_2", "TELEFONE_2", "DDD_DO_FAX", "FAX", "CORREIO_ELETRONICO",
        "SITUACAO_ESPECIAL", "DATA_DA_SITUACAO_ESPECIAL"
    ],
    'SIMPLES': [
        "CNPJ_BASICO", "OPCAO_PELO_SIMPLES", "DATA_DE_OPCAO_PELO_SIMPLES",
        "DATA_DE_EXCLUSAO_DO_SIMPLES", "OPCAO_PELO_MEI", "DATA_DE_OPCAO_PELO_MEI",
        "DATA_DE_EXCLUSAO_DO_MEI"
    ],
    'SOCIO': [
        "CNPJ_BASICO", "IDENTIFICADOR_DE_SOCIO", "NOME_DO_SOCIO", "CPF_DO_SOCIO",
        "QUALIFICACAO_DO_SOCIO", "DATA_DE_ENTRADA_SOCIEDADE", "PAIS",
        "REPRESENTANTE_LEGAL", "NOME_DO_REPRESENTANTE", "QUALIFICACAO_DO_REPRESENTANTE_LEGAL",
        "FAIXA_ETARIA"
    ],
    'PAIS': ["CODIGO", "DESCRICAO_PAIS"],
    'MUNIC': ["CODIGO", "DESCRICAO_MUNICIPIO"],
    'QUALS': ["CODIGO", "DESCRICAO_QUALIFICACAO"],
    'NATJU': ["CODIGO", "DESCRICAO_NATUREZA_JURIDICA"],
    'CNAE': ["CODIGO", "DESCRICAO_CNAE"]
}

# Conecta ao banco DuckDB
con = duckdb.connect(BANCO_DUCKDB)

# Lista todos os arquivos na pasta raw
arquivos = os.listdir(PASTA_RAW)
print(f"[INFO] {len(arquivos)} arquivos encontrados na pasta '{PASTA_RAW}'.")

for chave, colunas in cabecalhos.items():
    print(f"\n[PROCESSANDO] Chave: {chave}")
    
    arquivos_chave = [f for f in arquivos if chave.lower() in f.lower()]
    print(f"[INFO] {len(arquivos_chave)} arquivos encontrados para a chave '{chave}'.")
    
    if not arquivos_chave:
        print(f"[AVISO] Nenhum arquivo encontrado para a chave '{chave}'. Pulando...")
        continue

    # Cria a tabela vazia no DuckDB
    colunas_schema = ", ".join([f'"{c}" TEXT' for c in colunas])
    con.execute(f"CREATE OR REPLACE TABLE {chave} ({colunas_schema});")
    print(f"[INFO] Tabela '{chave}' criada no banco.")

    total_registros = 0

    for arq in arquivos_chave:
        caminho_arquivo = os.path.join(PASTA_RAW, arq)
        print(f"   [LENDO EM CHUNKS] {caminho_arquivo}")
        try:
            # Lê em pedaços de 100.000 linhas
            for chunk in pd.read_csv(caminho_arquivo, sep=';', header=None, dtype=str,
                                     encoding='ISO-8859-1', chunksize=100_000):
                chunk.columns = colunas
                con.execute(f"INSERT INTO {chave} SELECT * FROM chunk")
                total_registros += len(chunk)
                print(f"      [+] Inseridos {len(chunk)} registros (total: {total_registros})")
        except Exception as e:
            print(f"   [ERRO] Falha ao ler '{arq}': {e}")

    print(f"[SUCESSO] Tabela '{chave}' finalizada com {total_registros} registros.")

con.close()
print("\n[FINALIZADO] Processamento concluído.")
