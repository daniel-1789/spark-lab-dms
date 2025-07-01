from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("WordCount") \
        .master("local[*]") \
        .getOrCreate()

    data = ["hello world", "hello spark", "hello again"]
    df = spark.createDataFrame(data, "string").toDF("line")

    words = df.selectExpr("explode(split(line, ' ')) as word")
    counts = words.groupBy("word").count()

    counts.show()

    spark.stop()

if __name__ == "__main__":
    main()

