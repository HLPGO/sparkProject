from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col


class SparkTransformation:
    def __init__(self, data, columns):
        # Создаем сессию Spark
        self.spark = SparkSession.builder \
            .appName("SparkTransformation") \
            .getOrCreate()

        # Создаем DataFrame на основе переданных данных и колонок
        self.df = self.spark.createDataFrame(data, columns)

    def add_full_name_column(self):
        # Используем функцию concat_ws для объединения значений колонок name и surname с пробелом
        self.df = self.df.withColumn("full_name", concat_ws(" ", col("name"), col("surname")))
        return self.df

    def show_dataframe(self):
        # Выводим содержимое DataFrame
        self.df.show()

    def stop_spark_session(self):
        # Закрываем сессию Spark
        self.spark.stop()


# Пример использования:
if __name__ == "__main__":
    # Создаем экземпляр класса SparkTransformation, передавая данные и колонки
    data = [("John", "Doe"), ("Jane", "Smith"), ("Bob", "Brown")]
    columns = ["name", "surname"]
    spark_transformation = SparkTransformation(data, columns)

    # Добавляем новую колонку full_name
    spark_transformation.add_full_name_column()

    # Выводим результат
    spark_transformation.show_dataframe()

    # Закрываем сессию Spark
    spark_transformation.stop_spark_session()