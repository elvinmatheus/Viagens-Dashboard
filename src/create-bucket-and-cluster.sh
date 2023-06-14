#!/bin/bash

# Cria bucket
aws s3 mb s3://viagens-a-servico-gov --region sa-east-1

# Cria pastas no bucket 
aws s3api put-object --bucket s3://viagens-a-servico-gov --key 2023/
aws s3api put-object --bucket s3://viagens-a-servico-gov/2023 --key raw-data/
aws s3api put-object --bucket s3://viagens-a-servico-gov --key processed-data/

# Cria cluster gratuito no Redshift 
aws redshift create-cluster --cluster-identifier viagens-cluster --node-type dc2.large --master-username myuser --master-user-password mypassword --db-name viagens-DW --cluster-type single-node --availability-zone sa-east-1 --publicly-accessible

# Verifica status cluster
aws redshift describe-clusters --cluster-identifier viagens-DW-cluster

# Desligar cluster
# aws redshift delete-cluster --cluster-identifier viagens-DW-cluster --skip-final-cluster-snapshot

# Cria tabela 
aws redshift create-table \
  --cluster-identifier viagens-cluster \
  --database-name viagens-DW \
  --table-name fatoviagens \
  --column-definitions "Identificador do processo de viagem integer,
                        Número da Proposta (PCDP) varchar(255),
                        Sequência Trecho integer,
                        Origem - Data date,
                        Origem - País varchar(255),
                        Origem - UF varchar(255),
                        Origem - Cidade varchar(255),
                        Destino - Data date,
                        Destino - País varchar(255),
                        Destino - UF varchar(255),
                        Destino - Cidade varchar(255),
                        Missao? varchar(255),
                        Meio de transporte varchar(255),
                        Valor da passagem float,
                        Taxa de serviço float,
                        Data da emissão/compra date,
                        Hora da emissão/compra time,
                        Situação varchar(255),
                        Viagem Urgente boolean,
                        Justificativa Urgência Viagem varchar(255),
                        Código órgão solicitante integer,
                        Nome órgão solicitante varchar(255),
                        CPF viajante varchar(255),
                        Nome varchar(255),
                        Cargo varchar(255),
                        Função varchar(255),
                        Descrição Função varchar(255),
                        Período - Data de início date,
                        Período - Data de fim date,
                        Destinos varchar(255),
                        Motivo varchar(255),
                        Código do órgão superior integer,
                        Nome do órgão superior varchar(255),
                        Codigo do órgão pagador integer,
                        Nome do órgao pagador varchar(255),
                        Código da unidade gestora pagadora integer,
                        Nome da unidade gestora pagadora varchar(255)"