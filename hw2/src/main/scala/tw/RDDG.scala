package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDG {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
     if (args.length != 2) {
      logger.error("Usage:\nmain.followCount <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follow Count").setMaster("local[4]").set("spark.executor.memory", "1g");
    conf.set("spark.hadoop.validateOutputSpecs", "false");
    val sc = new SparkContext(conf)
    //load textfile into spark
    val textFile = sc.textFile(args(0))
    val result = textFile.flatMap(line => line.split(" "))
                         .map(word => (word.split(",")(1), 1))
                         .groupByKey() //group by key will transform the RDD into
                                       //tuples with key and all its values in an iterable
                         .map(eachRecord => (eachRecord._1, eachRecord._2.sum))
                         //the above map function aggregates all the values in each key's iterable to get the sum of followers
    result.saveAsTextFile(args(1))
    println(result.toDebugString)
    
    
  }
}