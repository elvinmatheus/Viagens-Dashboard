# Viagens Dashboard

## Visão Geral

Este documento descreve o projeto de um dashboard que utiliza os dados de viagens do governo brasileiro, disponibilizado pelo Portal da Transparência. O projeto tem como objetivo coletar os dados de viagens no ano de 2023 do Portal da Transparência, exportá-los como arquivos CSV para um bucket do Amazon S3 (Staging Area), baixá-los do S3, processá-los com pandas, enviar os dados processados no formato CSV de volta ao bucket S3, copiar os dados para o Amazon Redshift e criar um dashboard dos dados no Redshift com o Google Looker Studio.

## Arquitetura do projeto

![Imagem representando a arquitetura do projeto](https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Arquitetura.png)

1. **Extração de Dados:** Nesta etapa, os dados são coletados a partir do website do Portal da Transparência, uma vez que os dados disponíveis lá são mais completos do que os fornecidos pela API do Portal da Transparência. As ferramentas utilizadas para a coleta dos dados foram as bibliotecas `requests` , `zipfile` e `io` do `Python`.

2. **Staging Area:** Após a coleta, os dados são exportados como arquivos CSV para um bucket do `Amazon S3`. Para a interação com o S3 é utilizado a biblioteca `s3fs` do `Python`.

3. **Extração da Staging Area e Processamento:** Os dados presentes na Staging Area são extraídos e, em seguida, processados usando a biblioteca `pandas`. 

4. **Armazenamento de Dados:** Os dados processados são armazenados como um único arquivo CSV em um bucket do `Amazon S3`. 

5. **Criação do Data Warehouse:** Os dados processados são carregados para um cluster do `Amazon Redshift`.

<p align="center">
  <img width="360" height="699" src="https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Tabela%20desnormalizada.png">
</p>

6. **Elaboração do Dashboard:** Os dados armazenados no cluster do Redshift são usados para criar Dashboards no `Google Looker Studio`.

- *Países mais visitados* 
![Países mais visitados](https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Pa%C3%ADses%20mais%20visitados.png)

- *Viagens por órgão* 
![Viagens por órgão](https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Viagens%20por%20%C3%93rg%C3%A3o.png)

7. **Agendamento e Orquestração:** O agendamento e orquestração do pipeline são realizados usando o `Apache Airflow`.

![Pipeline dados](https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Pipeline.png)

## Configuração e execução do pipeline

1. **Criação da instância no EC2:** É preciso ter o [`AWS CLI` instalado localmente e configurado corretamente](https://docs.aws.amazon.com/pt_br/cli/latest/userguide/getting-started-install.html) para criar a instância no EC2. Execute os seguintes comandos:

```
sudo chmod +x create_ec2_instance.sh
./create_ec2_instance.sh
```

2. **Configuração do ambiente:** É necessário configurar o ambiente de de desenvolvimento na instância previamente criada com as bibliotecas e dependências necessárias, como o sistema de gerenciamento de pacotes `pip`, `pandas`. `requests`, etc. Para isso, você deve se [conectar à instância criada](https://docs.aws.amazon.com/pt_br/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) e executar os seguintes comandos. [Instale também o AWS CLI na instância criada](https://docs.aws.amazon.com/pt_br/cli/latest/userguide/getting-started-install.html):

```
# Na instância EC2 criada
sudo apt-get update
sudo apt install python3-pip
sudo pip install apache-airflow
sudo pip install pandas
sudo pip install s3fs
```

3. **Configuração do Airflow para a execução do pipeline:** Envie a pasta `/src` para a pasta `airflow` na instância criada. Altere a linha 4 do arquivo `airflow.cfg`, de modo que a variável `dags_folder` aponte para a pasta `/home/ubuntu/airflow/src`.

4. **Configuração das Credenciais:** É necessário configurar as credenciais de acesso ao Amazon S3, [criando uma nova role com as regras de acesso ao S3](https://docs.aws.amazon.com/pt_br/IAM/latest/UserGuide/id_roles_create_for-service.html).

5. **Execução do Pipeline:** Use o comando `airflow standalone` no terminal da instância EC2 criada. O bucket S3 e o cluster no Amazon Redshift serão criados automaticamente. O pipeline de dados será executado um dia após a criação do bucket e do cluster, e repetirá uma vez por mês.

6. **Dashboard no Google Looker Studio:** Acesse [lookerstudio.google.com](lookerstudio.google.com).

    1. Crie um novo relatório

        ![Tela inicial google looker studio](https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Tela%20inicial%20Google%20Looker%20Studio.png)

    2. Escolha o Amazon Redshift como fonte de dados

        ![Fonte de dados](https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Escolher%20Fonte%20de%20Dados.png)

    3. Configure o acesso do Google Looker Studio ao cluster do Redshift. (É preciso permitir acesso ao cluster dentro do Redshift)

        ![Configurar acessso ao Redshift](https://github.com/elvinmatheus/viagens-Dashboard/blob/main/imagens/Configurar%20fonte%20de%20dados.png)

    4. Após a conexão, escolha a tabela fatovendas e comece a fazer suas análises. :-)