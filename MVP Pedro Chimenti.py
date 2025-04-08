# Databricks notebook source
# DBTITLE 1,Criando camada bronze
# MAGIC %sql
# MAGIC create database bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Importei diretamento para a camada bronze do meu computador uma planilha de veiculos eletricos nos estados unidos.

# COMMAND ----------

# DBTITLE 1,Exemplo da tabela na camada bronze
# MAGIC %sql 
# MAGIC SELECT * FROM bronze.electric_vehicle_population_data_8_csv LIMIT 10

# COMMAND ----------

# DBTITLE 1,Criação da camada silver
# MAGIC %sql CREATE DATABASE silver

# COMMAND ----------

# DBTITLE 1,Ingestao e tratamento da tabela na camada silver
# MAGIC %sql 
# MAGIC     create table silver.Eletricvehicle stored as ORC as 
# MAGIC         (select County, City, State, `Model Year`,Make, Model, `Electric Vehicle Type`, `Electric Range`, `DOL Vehicle ID`  
# MAGIC          from bronze.electric_vehicle_population_data_8_csv
# MAGIC          Where County is not NULL and City is not NULL);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Fiz a transferencia da tabela para a camada Silver e fiz uma limpeza nas colunas Conty, City e Eletric Range

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM silver.eletricvehicle LIMIT 10

# COMMAND ----------

# MAGIC %sql CREATE DATABASE golden

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table golden.eletricmedia stored as ORC as 
# MAGIC     select `Electric Vehicle Type`, AVG(`Electric Range`) as Mediadedistancia
# MAGIC     from silver.eletricvehicle
# MAGIC     group by `Electric Vehicle Type`

# COMMAND ----------

# DBTITLE 1,Quantidade de hibrido e eletrico por estado
# MAGIC %sql
# MAGIC create table golden.QuantidadeXEstado stored as ORC as 
# MAGIC   SELECT
# MAGIC     `Electric Vehicle Type`,
# MAGIC     State,
# MAGIC     COUNT(*) AS QuantidadedeVeiculo
# MAGIC   FROM silver.eletricvehicle
# MAGIC   GROUP BY State,`Electric Vehicle Type`;

# COMMAND ----------

# DBTITLE 1,Quantidade de hibrido e eletrico por marca
# MAGIC %sql
# MAGIC create table golden.TipoXMontadora stored as ORC as 
# MAGIC   SELECT
# MAGIC     `Electric Vehicle Type`,
# MAGIC     Make,
# MAGIC     COUNT(*) AS QuantidadedeVeiculo
# MAGIC     FROM silver.eletricvehicle
# MAGIC     GROUP BY Make,`Electric Vehicle Type`;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table golden.QuantidadeXAno stored as ORC as 
# MAGIC   SELECT
# MAGIC     `Model Year`,
# MAGIC     COUNT(*) AS QuantidadedeVeiculo
# MAGIC   FROM silver.eletricvehicle
# MAGIC   GROUP BY `Model Year`;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table golden.QuantidadeXMontadora stored as ORC as 
# MAGIC   SELECT
# MAGIC     `Make`,
# MAGIC     COUNT(*) AS quantidadedeveiculo
# MAGIC   FROM silver.eletricvehicle
# MAGIC   GROUP BY `Make`;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table golden.eletricvehicle stored as ORC as 
# MAGIC   select *
# MAGIC   from silver.eletricvehicle
# MAGIC

# COMMAND ----------

# DBTITLE 1,Exemplo tabela fato na camada golden
# MAGIC %sql 
# MAGIC SELECT * FROM golden.eletricvehicle LIMIT 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC   FROM golden.tipoxmontadora LIMIT 10