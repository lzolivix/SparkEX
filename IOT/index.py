from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum as spark_sum

spark = SparkSession.builder.appName("DesafioFinalVendas").getOrCreate()

df_vendas = spark.read.csv("vendas.csv", header=True, inferSchema=True)

df_vendas.show()

clientes_top = df_vendas.orderBy(col("valor_compra").desc())
print("Clientes com maior valor de compra:")
clientes_top.show()

df_vendas_ano = df_vendas.withColumn("ano", year(col("data_compra")))
vendas_anuais = df_vendas_ano.groupBy("ano").agg(spark_sum("valor_compra").alias("total_vendas"))
print("Total de vendas por ano:")
vendas_anuais.show()

clientes_top.write.mode("overwrite").csv("resultado/clientes_top", header=True)
vendas_anuais.write.mode("overwrite").json("resultado/vendas_anuais")