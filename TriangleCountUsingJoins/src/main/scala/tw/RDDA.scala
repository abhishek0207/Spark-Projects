package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDA {
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
                         //aggregateByKey(initialValue)(SequentialFunction, Combiner function) 
                         //aggregating keys by taking 0 as the initial value of aggregation
                         //intermediate sequential function is the sum of values of each key
                         //combiner function across the tasks sums values for each key in different partition
                         .aggregateByKey(0)((x,y) => (x + y), (m,n)=> (m + n))
    result.saveAsTextFile(args(1))
    println(result.toDebugString)
    
    
  }
}