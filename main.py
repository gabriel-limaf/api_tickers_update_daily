import requests
from zipfile import ZipFile
from datetime import datetime, timedelta
from flask import Flask, jsonify
import json
from google.cloud import bigquery
import os

app = Flask(__name__)


# Realizar o download do arquivo da B3
def download_arquivo(url, destination):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    else:
        return jsonify({'mensagem': 'Nao foi possivel realizar o download do arquivo na B3'})

    return None


# Descompactar o arquivo da B3
def unzip(destination):
    with ZipFile(destination, 'r') as zip_file:
        zip_file.extractall()


# Realizar a leitura do arquivo txt e extrair as informações
def process_all_tickers(txt_path):
    processed_data = []
    with open(txt_path, "r", encoding='utf-8') as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if 'COTAHIST' in linha:
                continue
            else:
                if linha[10:12].strip() == "02" or "08":
                    codigo_bdi = int(linha[10:12].strip()),
                    data_pregao = datetime.strptime(linha[2:10], "%Y%m%d").date()
                    ticker = linha[12:24].strip()
                    preco_abertura = float(linha[56:69]) / 100
                    preco_maximo = float(linha[69:82]) / 100
                    preco_minimo = float(linha[82:95]) / 100
                    preco_medio = float(linha[95:108]) / 100
                    preco_fechamento = float(linha[108:121]) / 100
                    qnt_negociada = int(linha[152:170])
                    vol_negociado = float(linha[170:188]) / 100
                    processed_data.append({
                        "ticker": ticker,
                        "codigo_bdi": codigo_bdi,
                        "data_pregao": data_pregao.isoformat(),
                        "preco_abertura": preco_abertura,
                        "preco_maximo": preco_maximo,
                        "preco_minimo": preco_minimo,
                        "preco_medio": preco_medio,
                        "preco_fechamento": preco_fechamento,
                        "qnt_negociada": qnt_negociada,
                        "vol_negociado": vol_negociado
                    })
    return processed_data


# Incluir dados na tabela no Big Query
def big_query(processed_data, table, client):
    # Divida os dados em lotes de 1000 itens
    batch_size = 2000

    for i in range(0, len(processed_data), batch_size):
        batch = processed_data[i:i + batch_size]

        # Construa os valores para inserção
        values_part = ', '.join(
            [f"('{item['ticker']}', '{item['data_pregao']}', {item['preco_fechamento']}, {item['preco_maximo']},"
             f" {item['preco_medio']}, {item['preco_minimo']}, {item['preco_abertura']}, {item['qnt_negociada']}, "
             f"{item['vol_negociado']})" for item in batch])

        # Consulta para inserir dados
        query_insert = (
            f"INSERT INTO `{table}` "
            "(ticker, data_pregao, preco_fechamento, preco_maximo, preco_medio, preco_minimo, preco_abertura, "
            "qnt_negociada, vol_negociado) "
            f"VALUES {values_part}"
        )

        # Execute a consulta de inserção
        query_job = client.query(query_insert)
        query_job.result()  # Aguarda a conclusão da consulta


# Função para deletar arquivos
def delete_files(*files):
    for file in files:
        if os.path.exists(file):
            os.remove(file)
            print(f"Arquivo {file} deletado com sucesso.")
        else:
            print(f"O arquivo {file} não existe.")


# Chamada da API
@app.route('/stockprices', methods=['GET'])
def process_all_tickers_endpoint():
    yesterday = datetime.now() - timedelta(days=1)
    data_consulta = yesterday.strftime('%d%m%Y')
    data_consulta_query = yesterday.strftime("%Y-%m-%d")

    table = ''
    client = bigquery.Client()

    # Consulta para selecionar os dados inseridos
    query_select = (
        f"SELECT EXISTS (SELECT 1 FROM {table} "
        f"WHERE data_pregao = '{data_consulta_query}') LIMIT 1 "
    )

    # Execute a consulta de seleção
    query_job = client.query(query_select)
    rows = query_job.result()  # Aguarda a conclusão da consulta
    result = True or False
    for row in rows:
        result = row['f0_']

    # Verifique se a data existe na tabela
    if result == True:
        mensagem = f"A data {data_consulta_query} existe na tabela."
        return jsonify({'mensagem': mensagem}), 200
    if result == False:
        file_url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{data_consulta}.ZIP'
        destination = f'COTAHIST_D{data_consulta}.ZIP'
        txt_path = f'COTAHIST_D{data_consulta}.TXT'

        # Chamada para fazer o download do arquivo
        download_result = download_arquivo(file_url, destination)
        if download_result:
            return download_result

        # Se o download for bem-sucedido, continue com o processamento
        unzip(destination)
        processed_data = process_all_tickers(txt_path)
        big_query(processed_data, table, client)
        # Deletar os arquivos após finalizar o processamento
        delete_files(destination, txt_path)
        return json.dumps(processed_data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
