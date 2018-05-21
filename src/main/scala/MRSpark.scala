import org.apache.spark.{SparkConf, SparkContext}

object MRSpark {
  val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount")
  val sc = new SparkContext(conf)

  val textFile = sc.textFile("hdfs://...")

  val counts = textFile.flatMap(line => line.split(" "))
                       .map(word => (word, 1))
                       .reduceByKey(_ + _)

  counts.saveAsTextFile("hdfs://...")
}
