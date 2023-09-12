from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Инициализация SparkSession
spark = SparkSession.builder.getOrCreate()

# Создание датафрейма с продуктами и именем столбца "product"
products = spark.createDataFrame([("product1",), ("product2",), ("product3",)], ["product"])

# Создание датафрейма с категориями и именем столбца "category"
categories = spark.createDataFrame([("category1",), ("category2",), ("category3",)], ["category"])

# Решение задачи
result = products.crossJoin(categories)

# Переименование столбцов
result = result.withColumnRenamed("product", "Имя продукта").withColumnRenamed("category", "Имя категории")

# Показ результата
result.show()