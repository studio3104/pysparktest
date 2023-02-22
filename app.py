from pyspark.sql import SparkSession, DataFrameReader


class Hoge:
    def __init__(self) -> None:
        self.spark = SparkSession \
            .builder \
            .appName('Hoge') \
            .getOrCreate()

    def run(self) -> DataFrameReader:
        df = self.spark.read \
            .format('csv') \
            .option('header', 'true') \
            .option('inferScheme', 'true') \
            .load('hoge.csv')

        return df
