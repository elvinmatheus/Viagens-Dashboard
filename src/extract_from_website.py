import requests
import zipfile
import io

url = 'https://portaldatransparencia.gov.br/download-de-dados/viagens/2023'

def run_extract_from_website():
    
    response = requests.get(url)

    if response.ok:

        # Cria um objeto de fluxo de bytes a partir do conteúdo da resposta
        zip = io.BytesIO(response.content)

        # Extrai todos os arquivos do arquivo ZIP para o diretório local
        with zipfile.ZipFile(zip, 'r') as zip_file:
            zip_file.extractall('.')

        print("Arquivo zip baixado e descompactado com sucesso.")

    else:
        print("Falha ao baixar o arquivo zip.")