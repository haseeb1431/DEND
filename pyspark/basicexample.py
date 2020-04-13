from pyspark.sql import SparkSession



def sparkJob(self, arg):
    spark = SparkSession \
        .builder \
        .appName("Data Frames practice") \
        .getOrCreate()

    names = [
        "haseeb",
        "haseen",
        "haseena",
        "has",
        "hossein",
        "habib",
        "habibi",
        "hasiba"
    ]

    sc = spark.sparkContext

    namesRdd = sc.parallelize(names)

    print(namesRdd.map(lambda x: len(x)).collect())

    spark.stop



if __name__ == "__main__":
    sparkJob()