#!/bin/bash

# Cria a key-pair
aws ec2 create-key-pair --key-name viagens-key-pair

# Cria o Security Group
aws ec2 create-security-group --group-name SeuSecurityGroup --description "" --vpc-id SeuVPCId

# Cria uma regra para permitir o tráfego HTTP (porta 80) 
aws ec2 authorize-security-group-ingress --group-id SeuGroupID --protocol tcp --port 80 --cidr 0.0.0.0/0

# Cria uma regra para permitir o tráfego HTTPS (porta 443)
aws ec2 authorize-security-group-ingress --group-id SeuSecurityGroupID --protocol tcp --port 443 --cidr 0.0.0.0/0

# Cria uma regra para permitir o tráfego SSH (porta 22)
aws ec2 authorize-security-group-ingress --group-id SeuSecurityGroupID --protocol ssh --port 22 --cidr 0.0.0.0/0

# Cria uma regra para permitir o tráfego de qualquer fonte
aws ec2 authorize-security-group-ingress --group-id SeuSecurityGroupID --protocol all --cidr 0.0.0.0/0

# Cria a instância EC2 (no mínimo t3.medium)
aws ec2 run-instances --region sa-east-1 --image-id ami-0f47fe3e9defb4cbf --count 1 --instance-type t3.medium --key-name viagens-key-pair --security-group-ids SeuSecurityGroupID