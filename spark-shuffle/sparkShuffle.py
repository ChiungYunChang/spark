from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShuffleExample") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

data = [("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5), ("c", 6)]
df = spark.createDataFrame(data, ["key", "value"])

print("原始 partitions 數量：", df.rdd.getNumPartitions())

# 不會觸發 shuffle (filter)
filtered = df.filter(df.value > 2)
print("Filter 後 partitions 數量：", filtered.rdd.getNumPartitions())

# 觸發 shuffle 
grouped = df.groupBy("key").count()
print("GroupBy 後 partitions 數量：", grouped.rdd.getNumPartitions())

grouped.show()
