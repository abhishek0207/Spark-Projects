package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object RSJoinDataset {
  def main(args: Array[String]) {
  
  val logger: org.apache.log4j.Logger = LogManager.getRootLogger
     if (args.length != 2) {
      logger.error("Usage:\nmain.followCount <input dir> <output dir>")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("Twitter Follow Count").master("local[4]").getOrCreate()
    val TextFile = spark.read.format("csv").load(args(0)).toDF();
    var max = 10;
    val toData  = TextFile.filter(TextFile("_c0") < max && TextFile("_c1") < max)
    //val fromData = toData
    val Dataleft =  toData.as("to").join(toData.as("from")).where(col("to._c0") === col("from._c1"))
    .collect();
    val newData = Dataleft.map(eachRecord => (eachRecord.toString().split(",")(0), (eachRecord.toString().split(",")(1) + "," + eachRecord.toString().split(",")(2))))
    newData.foreach(println)
    //val newData = Dataleft.join(toData
   // filteredData.collect().foreach(println)
//    var maxValue: Int = 1000;
//    val toFile = TextFile
//                 .filter(record => Integer.parseInt(record.split(",")(0)) < maxValue && Integer.parseInt(record.split(",")(1)) < maxValue)
//                .map(record => (record.split(",")(0), record.split(",")(1)))
//    val fromFile = TextFile
//                 .filter(record => Integer.parseInt(record.split(",")(0)) < maxValue && Integer.parseInt(record.split(",")(1)) < maxValue)
//                .map(record => (record.split(",")(1), record.split(",")(0)))
//   
//    val result1 = toFile.join(fromFile).map(record => (record._2,1)) //gives path twos for a key
//    val toFilechanged = toFile.map(record => (record, 1))
//    var result2: Long = result1.join(toFilechanged).count();
//    println(result2 / 3);
    
  }
}