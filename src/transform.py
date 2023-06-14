import pandas as pd
import datetime as dt
import os

dag_path = os.getcwd()
today = dt.date.today()

def run_transform():

    arquivos = ['2023_Passagem.csv', '2023_Pagamento.csv', '2023_Trecho.csv', '2023_Viagem.csv']
    caminho = f'{dag_path}/raw-data/'

    for arquivo in arquivos:
        print(caminho + arquivo)
        if not os.path.exists(caminho + arquivo):
            raise FileNotFoundError(f'O arquivo {arquivo} não foi encontrado')

    passagem = pd.read_csv(f'{dag_path}/raw-data/2023_Passagem.csv', encoding='cp1252', sep=';', dtype={'Identificador do processo de viagem': str})
    pagamento = pd.read_csv(f'{dag_path}/raw-data/2023_Pagamento.csv', encoding='cp1252', sep=';', dtype={'Identificador do processo de viagem': str, 'Código do órgão superior': str, 'Codigo do órgão pagador': str, 'Código da unidade gestora pagadora': str})
    trecho = pd.read_csv(f'{dag_path}/raw-data/2023_Trecho.csv', encoding='cp1252', sep=';', dtype={'Identificador do processo de viagem ': str})
    viagem = pd.read_csv(f'{dag_path}/raw-data/2023_Viagem.csv', encoding='cp1252', sep=';', dtype={'Identificador do processo de viagem': str, 'Código órgão solicitante': str})

    pagamento.rename(columns={'Codigo do órgão pagador':'Código do órgão pagador'}, inplace=True)
    trecho.rename(columns={'Identificador do processo de viagem ':'Identificador do processo de viagem'}, inplace=True)
    trecho = trecho.drop('Meio de transporte', axis=1)

    merge1 = pd.merge(trecho, passagem, left_on=['Identificador do processo de viagem', 'Número da Proposta (PCDP)', 'Origem - Cidade', 'Origem - UF', 'Origem - País'], right_on=['Identificador do processo de viagem', 'Número da Proposta (PCDP)', 'Cidade - Origem ida', 'UF - Origem ida', 'País - Origem ida' ])

    merge1 = merge1.drop(['País - Origem ida', 'UF - Origem ida',
                        'Cidade - Origem ida', 'País - Destino ida', 'UF - Destino ida',
                        'Cidade - Destino ida', 'País - Origem volta', 'UF - Origem volta',
                        'Cidade - Origem volta', 'Pais - Destino volta', 'UF - Destino volta',
                        'Cidade - Destino volta', 'Número Diárias'], axis=1)

    viagem = viagem.drop(['Código do órgão superior', 'Nome do órgão superior', 'Valor diárias', 'Valor passagens', 'Valor devolução', 'Valor outros gastos'], axis=1)
    viagem.drop_duplicates(inplace=True)

    merge2 = pd.merge(merge1, viagem, on=['Identificador do processo de viagem', 'Número da Proposta (PCDP)'])

    pagamento = pagamento[pagamento['Tipo de pagamento'] == 'PASSAGEM']
    pagamento = pagamento.drop(['Tipo de pagamento', 'Valor'], axis=1)
    pagamento.drop_duplicates(subset=['Identificador do processo de viagem', 'Número da Proposta (PCDP)'], inplace=True, keep='first')

    merge3 = pd.merge(merge2, pagamento, on=['Identificador do processo de viagem', 'Número da Proposta (PCDP)'])

    merge3['Valor da passagem'] = merge3['Valor da passagem'].str.replace(',', '.')
    merge3['Valor da passagem'] = pd.to_numeric(merge3['Valor da passagem'])
    merge3['Taxa de serviço'] = merge3['Taxa de serviço'].str.replace(',', '.')
    merge3['Taxa de serviço'] = pd.to_numeric(merge3['Taxa de serviço'])

    processed_data_dir = f'{dag_path}/processed-data'
    if not os.path.exists(processed_data_dir):
        os.makedirs(processed_data_dir)

    merge3.to_csv(f'{processed_data_dir}/viagens-processed-data.csv', index=False)