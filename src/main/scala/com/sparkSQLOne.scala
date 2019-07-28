package com
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.reflect.io.Directory
import java.io.File

object sparkSQLOne {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext("One");
    doWordCount(sc)
  }

  def doWordCount(sc: SparkContext) {
    val textFile = sc.textFile("./src/main/resources/wordCount.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    val directory = new Directory(new File("./wordCountOutput"))
    directory.deleteRecursively()
    counts.saveAsTextFile("wordCountOutput")
  }

  def getSparkContext(jobName: String): SparkContext = {
    val conf = new SparkConf().setAppName(jobName).setMaster("local[1]") /*.set("spark.driver.allowMultipleContexts", "true")*/
    val sc = new SparkContext(conf)
    sc
  }

}