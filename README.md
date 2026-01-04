# LUCAS_LOPES_DDF_TECH_012026
PROJETO DADOSFERA
## Item 0 - Sobre Agilidade e Planejamento
### Fluxograma do Projeto
    flowchart TD
    A[Fontes de Dados CSV / APIs] --> B[Ingestão Python / Colab]
    B --> C[Camada RAW]
    C --> D[Tratamento e Limpeza]
    D --> E[Camada TRUSTED]
    E --> F[Modelagem Analítica]
    F --> G[Camada REFINED]
    G --> H[Análises SQL / BI]


### Interdependências e Pontos Críticos
    Etapa	    |Dependência	    |Ponto Crítico
    Ingestão	|Fonte disponível	|Schema inconsistente
    Tratamento  |RAW correta	    |Perda de dados
    Modelagem	|TRUSTED estável	|Chaves mal definidas
    Análises	|REFINED correta	|Performance / custo


### Análise de Riscos
      Risco	                     |Impacto |Mitigação
      Dados incompletos	         |Alto	  |Validações automáticas
      Custo excessivo de queries |Médio	  |Particionamento
      Erro de modelagem	         |Alto	  |Revisão por camadas
      Dependência manual	     |Médio	  |Automação em Python

### Estimativa de Custos (Google Cloud Platform)

    Item	                    |Serviço GCP	                  |Custo Estimado
    Armazenamento de dados	    |Cloud Storage (CSV / Parquet)	  |Gratuito (até 5GB)
    Data Warehouse	            |BigQuery Storage	              |Gratuito (até 10GB)
    Processamento analítico	    |BigQuery Queries	              |Gratuito (até 1TB/mês)
    Ambiente de desenvolvimento	|Google Colab	                  |Gratuito
    Autenticação e IAM	        |GCP IAM	                      |Gratuito
Custo mensal estimado R$ 0,00

## Item 1 - Sobre a Base de Dados
Base de dados usada foi a Olist Brazilliam e comerce Database:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_geolocation_dataset.csv

Essa base contem pelo menos 8 tabelas sem tratamento e mais de 100mil registros como solicitado

## Item  2.1 - Sobre a Dadosfera - Integrar
Conexões: LUCAS_LOPES_DDF_TECH_012026

LINK https://app.dadosfera.ai/pt-BR/collect/connections/1767461702251_i8083wn5_postgresql-1.0.0

BANCO UTILIZADO : POSTGRES

São 3 Schemas RAW, TRUSTED E REFINED

RAW: base original direto da API eu mantenho essa base para caso tenha alguma mudança na api eu consiga ver facilmente

TRUSTED: base tratado de dados direto do python

REFINED: base de dados refinados com uso de 3 formas diferentes a base Historico usando python, a base de review_sentimento usando ML e base preco_frete usando SQL

## Item  3 - Sobre a Dadosfera - Explorar

Todos os dados dentro da dadosfera se encontram aqui: 

https://app.dadosfera.ai/pt-BR/collect/connections/1767461702251_i8083wn5_postgresql-1.0.0

## Item 4 - Sobre Data Quality
Sobre os dados temos alguns pontos sobre as tabelas irei falar sobre as 8 tabelas no schema raw:

Antes de tudo as tabelas possuem dados nulos e alguns foram do padrão como a imagem abaixo que mostra um parte do srcipt [DADOSFERA_PROJETO.ipynb](https://github.com/Lopes258/LUCAS_LOPES_DDF_TECH_012026/blob/main/DADOSFERA_PROJETO.ipynb)  que crie para extração e tratamento do dados:

<img width="347" height="435" alt="image" src="https://github.com/user-attachments/assets/d01cd955-f82c-4be5-8b26-7cfb680559d9" />


CUSTOMER: O cliente possui 2 ID para eles um para o pedido e outro o id unico dele então caso queira saber quantos produtos um cliente comprou precisa usar o "customer_unique_id" e não o "customer_id"

ORDER: O fato de se usar o "customer_id" e não o "customer_unique_id" pode gerar alguma confusão pois como cada "customer_id" é um codigo unico para cada order para saber se o mesmo cliente comprou precisamos usar o customer_unique_id

PAYMENT: Nessa tabela os dados de "order_id" podem ser duplicados pois caso o cliente use mais de um metodo de pagamento diferente para realizar o pagamento é criado um registro para cada tipo

REVIEWS: A tabela com mais dados nulos devido a ser sobre comentarios com 87656 registros sem dados na coluna "review_comment_title" e 58247 registros sem dados na coluna "review_comment_message"

ITEMS: Coluna de items sem nenhum dado nulo mas é uma coluna criada a partir da order, sellers e products é uma tabela que depende das outras 3 para exisitr

PRODUCTS: Tabela com as informações do produtos com 610 registros sem dados de altura, peso, largura e comprimento

SELLERS: Tabela com os dados do vendedores do e-commerce nenhum dado com erro ou nulo

GEOLOCATION: Tabela com dados de endereços de lat/long do BRASIL

<img width="9455" height="2858" alt="Blank diagram - Page 1 (2)" src="https://github.com/user-attachments/assets/006cda14-b95b-44ba-a561-4e9d7bf0d867" />

## Item 5 - Sobre o uso de GenAI e LLMs - Processar

Utilizando ML no script ir direto para a parte de Machine Learning pode ser possivel ver que utilizei na tabela de reviews para analisar os comentarios e identificar qual o real sentimento do cliente com o produto comprado

PARTE DO CODIGO CRIADO PARA ANALISE DE TEXTO

<img width="930" height="700" alt="image" src="https://github.com/user-attachments/assets/f19b6f0d-c52d-4b18-9699-ef4e2b6dffd5" />


EXEMPLO DE ANALISE DO ML CRIADO

<img width="654" height="407" alt="image" src="https://github.com/user-attachments/assets/239713d2-a8c9-46bd-83e5-d281e187c16f" />

## Item 6 - Sobre Modelagem de Dados

Toda a modelagem de dados segue nesse script: 

https://github.com/Lopes258/LUCAS_LOPES_DDF_TECH_012026/blob/main/DADOSFERA_PROJETO.ipynb

## Item  7 - Sobre Análise de Dados - Analisar

Toda a analise sobre os dados segue nesse link:

https://metabase-treinamentos.dadosfera.ai/dashboard/240-lucas-lopes-012026?tab=22

## Item  8 - Sobre Pipelines

CONEXÕES: https://app.dadosfera.ai/pt-BR/collect/connections/1767461702251_i8083wn5_postgresql-1.0.0

PIPELINE REFINED: https://app.dadosfera.ai/pt-BR/collect/pipelines/056a943e-d050-4057-9778-32ff031a863f

PIPELINE RAW : https://app.dadosfera.ai/pt-BR/collect/pipelines/7fbd3370-299a-4af0-9945-1b6a1bb6b12b

PIPELINE TRUSTED : https://app.dadosfera.ai/pt-BR/collect/pipelines/b41c4510-4cb2-4b49-8025-7ce7455b36ba


