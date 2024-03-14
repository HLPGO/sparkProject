from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

def add_full_name_column(df):
    # Используем функцию concat для объединения значений колонок name и surname с пробелом
    df_with_full_name = df.withColumn("full_name", concat_ws(" ", col("name"), col("surname")))
    return df_with_full_name

# Пример использования:
if __name__ == "__main__":
    # Создаем сессию Spark
    spark = SparkSession.builder \
        .appName("AddFullNameColumn") \
        .getOrCreate()

    # Создаем исходный DataFrame
    data = [("John", "Doe"), ("Jane", "Smith"), ("Bob", "Brown")]
    columns = ["name", "surname"]
    df = spark.createDataFrame(data, columns)

    # Добавляем новую колонку full_name
    df_with_full_name = add_full_name_column(df)

    # Выводим результат
    df_with_full_name.show()

    # Закрываем сессию Spark
    spark.stop()