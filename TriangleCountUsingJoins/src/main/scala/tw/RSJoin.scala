package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream

object RSJoin {
  def main(args: Array[String]) {
  
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
     if (args.length != 3) {
      logger.error("Usage:\nmain.followCount <input dir> <output dir><Max Value>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Reduce Side Join Spark").setMaster("local[4]").set("spark.executor.memory", "1g");
    conf.set("spark.hadoop.validateOutputSpecs", "false");
    val sc = new SparkContext(conf);
      val accum = sc.longAccumulator("counter");
    //load textfile into spark
    val TextFile = sc.textFile(args(0));
    var maxValue: Int = Integer.parseInt(args(2));
    val toFile = TextFile
                 .filter(record => Integer.parseInt(record.split(",")(0)) < maxValue && Integer.parseInt(record.split(",")(1)) < maxValue)
                .map(record => (record.split(",")(0), record.split(",")(1)))
    val fromFile = TextFile
                 .filter(record => Integer.parseInt(record.split(",")(0)) < maxValue && Integer.parseInt(record.split(",")(1)) < maxValue)
                .map(record => (record.split(",")(1), record.split(",")(0)))
   
    val result1 = toFile.join(fromFile).map(record => (record._2,1)) //gives path twos for a key
    val toFilechanged = toFile.map(record => (record, 1))
    var finalResult = result1.join(toFilechanged).count()
     val fos = new FileOutputStream(new File(args(1)))
    println(finalResult / 3)
     Console.withOut(fos) {finalResult}
    
    
  }
}