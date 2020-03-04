from pyspark imoprt SparkConf, SparkContext

conf = (SparkConf()
        .setMaster("local")
        .setAppName("John EMR Test")
        )

sc = SparkContext(conf = conf)


