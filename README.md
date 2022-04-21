# Data Lake Serveless - V0
O projeto foi realizado na cloud da AWS, se beneficiando do serviços em free tier.

## Requisitos e informações
Replicar as visualizações do site “https://covid.saude.gov.br/”, porém acessando
diretamente a API de Elastic.

Link oficial para todas as informações:
https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao

Informações para se conectar ao cluster:

- URL https://imunizacao-es.saude.gov.br/_search
- Nome do índice: desc-imunizacao
- Credenciais de acesso
    - o Usuário: imunizacao_public
    - o Senha: qlto5t&7r_@+#Tlstigi

## Stack

- Lambda
- S3
- Glue
- EC2 / Metabase

# Lambda
A ideia de utilizar a Lambda é conseguir fazer a extração de todos os dados paralelamente referente ao ano de 2022. Ao total serão duas Lambdas.

### covid-intake

Lambda que pega os dados de acordo com a data configurada no payload.

### covid-splitter

Lambda que irá replicara covid-intake de acordo com os dias totais de 2022, ou seja, acima de 100 lambdas invocadas.