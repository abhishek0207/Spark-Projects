package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object DSET {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
     if (args.length != 2) {
      logger.error("Usage:\nmain.followCount <input dir> <output dir>")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("Twitter Follow Count").master("local[4]").getOrCreate()
    //load textfile into spark
    val twitterDS = spark.read.format("csv").load(args(0));
    //println(result.toDebugString)
    val result = twitterDS.groupBy(twitterDS.col("_c1")).count()
    println(result.queryExecution.logical)
    //result.write.csv(args(1))
    //println(result.explain())
   
  }
}