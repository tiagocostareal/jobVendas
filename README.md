# Criação de um Data Lake com Dados de Vendas para consultas - Exercício da Formação Engenheiro de Dados AWS

#Criação de um data Lake

Criar IAM role

Glue: Criar Banco de Dados
Criar e executar Crawler
Criar Tabelas associadas ao Banco de Dados : Glue Data Catalog

Criar e executar Job
Transformar Dados
Tratamento de nomes de campos
Joins
Salvar no S3 como parquet com Partição (Status)
Criar Banco de Dados e Crawer para o Data Lake

---------------------------------------------------------------------------------------------------------------------------
1 Etapa: Crianção da função IAM para que o Glue possa executar funções no AWS durante sua execução. 
2 Etapa : Criação Bucket no S3
Criação de Pastas
  .Pasta chamada DataLake onde ficarão os dados transformados
  .Pasta chamada Temp Onde ficarão dados temporários 
  .Pasta chamada Log para os logs
  .Pasta chamada Script onde ficarão salvos os scritps no processo de ETL
  .Pasta chamada  SourceData onde ficarão os dados de origem

3 Etapa : Criação Database no Glue  
   .Criação Crawler e tabelas 

4 - Etapa : Transformação ETL
  .Criação e e execução do Job


