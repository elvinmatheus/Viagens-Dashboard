import pandas as pd
import datetime as dt

passagem = pd.read_csv('new-2023_Passagem.csv', encoding='cp1252', sep=';')
pagamento = pd.read_csv('new-2023_Pagamento.csv', encoding='cp1252', sep=';')
trecho = pd.read_csv('new-2023_Trecho.csv', encoding='cp1252', sep=';')
viagem = pd.read_csv('new-2023_Viagem.csv', encoding='cp1252', sep=';')

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

# Formatar dataframe
merge3['Identificador do processo de viagem'] = merge3['Identificador do processo de viagem'].astype('str')
merge3['Identificador do processo de viagem'] = '00000000000' + merge3['Identificador do processo de viagem']

merge3['Valor da passagem'] = merge3['Valor da passagem'].str.replace(',', '.')
merge3['Valor da passagem'] = pd.to_numeric(merge3['Valor da passagem'])
merge3['Taxa de serviço'] = merge3['Taxa de serviço'].str.replace(',', '.')
merge3['Taxa de serviço'] = pd.to_numeric(merge3['Taxa de serviço'])

merge3['Origem - Data'] = pd.to_datetime(merge3['Origem - Data'], format='%d/%m/%Y', dayfirst=True).dt.date
merge3['Destino - Data'] = pd.to_datetime(merge3['Destino - Data'], format='%d/%m/%Y', dayfirst=True).dt.date
merge3['Período - Data de início'] = pd.to_datetime(merge3['Período - Data de início'], format='%d/%m/%Y', dayfirst=True).dt.date
merge3['Período - Data de fim'] = pd.to_datetime(merge3['Período - Data de fim'], format='%d/%m/%Y', dayfirst=True).dt.date
merge3['Data da emissão/compra'] = pd.to_datetime(merge3['Data da emissão/compra'], format='%d/%m/%Y', dayfirst=True).dt.date

merge3.to_csv('viagem-processed-data.csv')