from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, mean

spark = SparkSession.builder.appName("DiabetesDataProcessing").getOrCreate()

df = spark.read.csv("projeto-1/dataset/diabetes.csv", header=True, inferSchema=True)

df.printSchema()
df.show(5)

# Estatísticas Descritivas e Verificação de Tipos de Dados
df.describe().show()
print(df.dtypes)

# Verifico se há valores nulos... nesse dataset infelizmente não há
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Preencho valores faltantes com a média, somente os valores com 0 serão já que não tem valores nulos
columns_to_impute = ["Glucose", "BloodPressure", "SkinThickness", "Insulin", "BMI"]
for column in columns_to_impute:
    mean_value = df.select(mean(df[column])).collect()[0][0]
    df = df.na.fill({column: mean_value})
    df = df.withColumn(column, when(df[column] == 0, mean_value).otherwise(df[column]))

# Filtrar registros inválidos (por exemplo, idades negativas) que neste dataset não há necessidade
df = df.filter(df['Age'] > 0)

df.show(5)

# Salvar o DataFrame tratado 
# df.write.csv("projeto-1/dataset/cleaned_diabetes.csv", header=True)

spark.stop()
