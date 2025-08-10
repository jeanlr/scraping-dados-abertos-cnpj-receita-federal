# %%
import requests
from bs4 import BeautifulSoup
import os
import zipfile

def listar_links_zip(url):
    resposta = requests.get(url)
    resposta.raise_for_status()
    soup = BeautifulSoup(resposta.text, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.endswith('.zip'):
            links.append(href if href.startswith('http') else url + href)
    return links

def baixar_arquivo(url, destino):
    resposta = requests.get(url, stream=True)
    resposta.raise_for_status()
    with open(destino, 'wb') as f:
        for chunk in resposta.iter_content(chunk_size=8192):
            f.write(chunk)

def descompactar_zip(caminho_zip, pasta_destino):
    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
        zip_ref.extractall(pasta_destino)

def remover_zips(pasta):
    for arquivo in os.listdir(pasta):
        if arquivo.endswith('.zip'):
            os.remove(os.path.join(pasta, arquivo))        

def baixar_e_descompactar_zips(url, pasta_destino):
    os.makedirs(pasta_destino, exist_ok=True)
    links = listar_links_zip(url)
    for link in links:
        nome_arquivo = link.split('/')[-1]
        caminho_zip = os.path.join(pasta_destino, nome_arquivo)
        print(f'Baixando {nome_arquivo}...')
        baixar_arquivo(link, caminho_zip)
        print(f'Descompactando {nome_arquivo}...')
        descompactar_zip(caminho_zip, pasta_destino)
    remover_zips(pasta_destino)
    print('Processo concluído.')

if __name__ == '__main__':
    url_base = 'https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/2023-05/'
    pasta_destino = 'raw'
    baixar_e_descompactar_zips(url_base, pasta_destino)