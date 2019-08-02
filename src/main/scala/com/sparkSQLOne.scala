package com
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.reflect.io.Directory
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object sparkSQLOne {

  def main(args: Array[String]): Unit = {
    //SparkSession
    val ss = SparkSession.builder().appName("SparkOne").master("local[1]").getOrCreate();

    val sc = getSparkContext(ss);
    sc.setLogLevel("ERROR");
    val sqlContext = getSparkSQLContext(ss)
    //rddOne(sc)
    //doWordCountRDDTwo(sc)
    //dataFrameOne(sqlContext)
    //dataFrameTwo(sqlContext)
    dataSetOne(sqlContext)
    println(sc.isStopped)
    sc.stop()
    println(sc.isStopped)
  }

  def getSparkContext(ss: SparkSession): SparkContext = {
    ss.sparkContext
  }
  def getSparkSQLContext(ss: SparkSession): SQLContext = {
    ss.sqlContext
  }

  case class Customer(name: String, age: Long)

  def dataSetOne(sqlContext: SQLContext) {
    import sqlContext.implicits._
    val dsOne = Seq(1, 2, 3).toDS()
    dsOne.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    val dsTwo = Seq(Customer("Andy", 32)).toDS()
    
    val path = "./src/main/resources/customer.json"
    val customer = sqlContext.read.json(path)//.as[Customer]
    customer.show()
    customer.printSchema()
    println(customer)

    val ds = sqlContext.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true").csv("./src/main/resources/customer.csv").as[Customer]
    ds.show()
    ds.printSchema()
    ds.select("age").show
    
  }

  def dataFrameTwo(sqlContext: SQLContext) {
    // https://stackoverflow.com/questions/31508083/difference-between-dataframe-dataset-and-rdd-in-spark/39033308
    // RDD = Distributed + Immutable + Fault tolerant (using lineage graph - DAG) + Lazy evaluations (All transformations in Spark are lazy evaluated, until action call on them)
    // DataFrame is simply a type alias of Dataset[Row]
    // spark 1.0 - RDD - compile-time type-safety
    // Spark 1.3 - DataFrame API has catalyst optimizer and Tungsten execution engine and to manage the schema
    // Spark 1.6 - DataSet API  - dataset.filter(_.age < 21);
    import sqlContext.implicits._
    val df = sqlContext.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true").csv("./src/main/resources/customer.csv")
    // Displays the content of the DataFrame to stdout
    df.show()
    df.printSchema()
    df.select($"name", $"age" + 1, $"job").show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("customer")
    val sqlDF = sqlContext.sql("SELECT * FROM customer")
    sqlDF.show()

  }

  def dataFrameOne(sqlContext: SQLContext) {
    // https://stackoverflow.com/questions/31508083/difference-between-dataframe-dataset-and-rdd-in-spark/39033308
    // RDD = Distributed + Immutable + Fault tolerant (using lineage graph - DAG) + Lazy evaluations (All transformations in Spark are lazy evaluated, until action call on them)
    // DataFrame is simply a type alias of Dataset[Row]
    // spark 1.0 - RDD - compile-time type-safety
    // Spark 1.3 - DataFrame API has catalyst optimizer and Tungsten execution engine and to manage the schema
    // Spark 1.6 - DataSet API  - dataset.filter(_.age < 21);
    import sqlContext.implicits._
    val df = sqlContext.read.json("./src/main/resources/customer.json")
    // Displays the content of the DataFrame to stdout
    df.show()
    df.printSchema()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("customer")
    val sqlDF = sqlContext.sql("SELECT * FROM customer")
    sqlDF.show()

  }

  def doWordCountRDDTwo(sc: SparkContext) {
    val textFile = sc.textFile("hdfs://localhost:9000/tmp/wordCount.txt")

    // map VS flatMap
    val mapOne = sc.textFile("./src/main/resources/wordCount.txt").map(l => l.split(" "))
    val flatMapOne = sc.textFile("./src/main/resources/wordCount.txt").flatMap(line => line.split(" "))
    mapOne.foreach(t => t.foreach(println))
    flatMapOne.foreach(println)

    // groupByKey VS reduceByKey
    // https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
    val mapRDD = textFile.flatMap(line => line.split(" ")).map(word => (word, 1))
    var counts = mapRDD.groupByKey().map(t => (t._1, t._2.sum))
    // it can combine output with a common key on each partition before shuffling the data
    counts = mapRDD.reduceByKey((x, y) => (x + y))

    val dir = counts.sortByKey();
    val directory = new Directory(new File("./wordCountOutput"))
    directory.deleteRecursively()
    dir.saveAsTextFile("wordCountOutput")
  }

  /**
   * There are two ways to create RDDs: parallelizing an existing collection in your driver program,
   * or referencing a data-set in an external storage system(HDGS, S3 ,etc.).
   */
  def rddOne(sc: SparkContext) {
    // RDDs are strong typing
    // Resilient Distributed Dataset
    // No. of Partitions = No. of tasks
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data) //ParallelCollectionRDD
    distData.foreach(println) // local mode
    distData.map(println) // local mode
    distData.collect().foreach(println) // cluster mode (.collect() is a action)
    distData.take(3).foreach(println) // if you only need to print a few elements of the RDD
    //which takes two arguments and returns one , commutative functions
    val count = distData.reduce((a, b) => a + b)
    println("count : " + count)

  }
}
