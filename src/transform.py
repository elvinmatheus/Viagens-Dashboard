import pandas as pd
import os

dag_path = os.getcwd()

trecho_columns = ['id_viagem', 'numero_da_proposta', 'sequencia_trecho', 'origem_data', 'origem_pais', 'origem_uf', 'origem_cidade', 'destino_data', 
                  'destino_pais', 'destino_uf', 'destino_cidade', 'meio_de_transporte', 'numero_diarias', 'em_missao']

passagem_columns = ['id_viagem', 'numero_da_proposta', 'meio_de_transporte', 'pais_origem_ida', 'uf_origem_ida', 'cidade_origem_ida', 'pais_destino_ida',
                    'uf_destino_ida', 'cidade_destino_ida', 'pais_origem_volta', 'uf_origem_volta', 'cidade_origem_volta',
                    'pais_destino_volta', 'uf_destino_volta', 'cidade_destino_volta', 'valor_da_passagem', 'taxa_de_servico', 'data_emissao_compra', 'hora_emissao_compra']

viagem_columns = {'Identificador do processo de viagem':'id_viagem', 'Número da Proposta (PCDP)':'numero_da_proposta', 'Situação':'situacao', 'Viagem Urgente':'viagem_urgente', 
                  'Justificativa Urgência Viagem':'justificativa_urgencia_viagem', 'Código órgão solicitante':'codigo_orgao_solicitante', 'Nome órgão solicitante':'nome_orgao_solicitante',
                  'CPF viajante':'cpf_viajante', 'Nome':'nome', 'Cargo':'cargo', 'Função':'funcao', 'Descrição Função':'descricao_funcao', 'Período - Data de início':'periodo_data_inicio', 
                  'Período - Data de fim':'periodo_data_fim', 'Destinos':'destinos', 'Motivo':'motivo'}

pagamento_columns = ['id_viagem', 'numero_da_proposta', 'codigo_orgao_superior', 'nome_orgao_superior', 'codigo_orgao_pagador', 'nome_orgao_pagador', 'codigo_unidade_gestora_pagadora',
                     'nome_unidade_gestora_pagadora', 'tipo_pagamento', 'valor']

def run_transform():

    arquivos = ['2023_Passagem.csv', '2023_Pagamento.csv', '2023_Trecho.csv', '2023_Viagem.csv']
    caminho = f'{dag_path}/raw-data/'

    for arquivo in arquivos:
        print(caminho + arquivo)
        if not os.path.exists(caminho + arquivo):
            raise FileNotFoundError(f'O arquivo {arquivo} não foi encontrado')

    trecho = pd.read_csv(f'{dag_path}/raw-data/2023_Trecho.csv', encoding='cp1252', sep=';', dtype={'Identificador do processo de viagem ': str}, header=0, names=trecho_columns)
    trecho.fillna('UF/Estado/Província estrangeira', inplace=True)
    trecho.origem_pais = trecho.origem_pais.str.title()
    trecho.destino_pais = trecho.destino_pais.str.title()
    trecho.origem_cidade = trecho.origem_cidade.str.title()
    trecho.destino_cidade = trecho.destino_cidade.str.title()

    passagem = pd.read_csv(f'{dag_path}/raw-data/2023_Passagem.csv', encoding='cp1252', sep=';', dtype={'Identificador do processo de viagem ': str}, header=0, names=passagem_columns)
    passagem.drop('meio_de_transporte', axis=1, inplace=True)

    merge1 = pd.merge(trecho, passagem, left_on=['id_viagem', 'numero_da_proposta', 'origem_cidade', 'origem_uf', 'origem_pais'], 
                                        right_on=['id_viagem', 'numero_da_proposta', 'cidade_origem_ida', 'uf_origem_ida', 'pais_origem_ida'])
    merge1.drop(['pais_origem_ida', 'uf_origem_ida', 'cidade_origem_ida', 'pais_destino_ida', 'uf_destino_ida', 'cidade_destino_ida', 'pais_origem_volta', 
                 'uf_origem_volta', 'cidade_origem_volta', 'pais_destino_volta', 'uf_destino_volta', 'cidade_destino_volta', 'numero_diarias'], axis=1, inplace=True)
    merge1.fillna(method='ffill', axis=0, inplace=True)

    viagem = pd.read_csv(f'{dag_path}/raw-data/2023_Viagem.csv', encoding='cp1252', sep=';', dtype={'Identificador do processo de viagem': str, 'Código órgão solicitante': str}, usecols=[0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17])
    viagem.rename(columns=viagem_columns, inplace=True)
    viagem.justificativa_urgencia_viagem = viagem.justificativa_urgencia_viagem.fillna("Sem informação")
    viagem.cpf_viajante = viagem.cpf_viajante.fillna("Informação protegida por sigilo")
    viagem.descricao_funcao = viagem.descricao_funcao.fillna("Sem informação")
    viagem.cargo = viagem.cargo.fillna("Sem informação")

    merge2 = pd.merge(merge1, viagem, on=['id_viagem', 'numero_da_proposta'])
    
    pagamento = pd.read_csv(f'{dag_path}/raw-data/2023_Pagamento.csv', encoding='cp1252', sep=';', dtype={'id_viagem': str, 'codigo_orgao_superior': str, 'codigo_orgao_pagador': str, 'codigo_unidade_gestora_pagadora': str}, header=0, names=pagamento_columns)
    pagamento.rename(columns={'Codigo do órgão pagador':'Código do órgão pagador'}, inplace=True)
    pagamento = pagamento[pagamento.tipo_pagamento == 'PASSAGEM'] 
    pagamento.drop(['tipo_pagamento', 'valor'], axis=1, inplace=True)
    pagamento.drop_duplicates(subset=['id_viagem', 'numero_da_proposta'], inplace=True, keep='first')
    pagamento.nome_orgao_superior = pagamento.nome_orgao_superior.str.title()
    pagamento.nome_orgao_pagador = pagamento.nome_orgao_pagador.str.title()
    pagamento.nome_orgao_pagador = pagamento.nome_orgao_pagador.fillna("Sem Informação")

    merge3 = pd.merge(merge2, pagamento, on=['id_viagem', 'numero_da_proposta'])

    processed_data_dir = f'{dag_path}/processed-data'
    if not os.path.exists(processed_data_dir):
        os.makedirs(processed_data_dir)

    merge3.to_csv(f'{processed_data_dir}/viagens-processed-data.csv', index=False)