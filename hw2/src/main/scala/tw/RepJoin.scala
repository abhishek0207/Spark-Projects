package tw

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import java.util.List

object RepJoin {
  def main(args: Array[String]) {
  
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
     if (args.length != 2) {
      logger.error("Usage:\nmain.followCount <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Reduce Side Join Spark").setMaster("local[4]").set("spark.executor.memory", "1g");
    conf.set("spark.hadoop.validateOutputSpecs", "false");
    val sc = new SparkContext(conf);
     val accum = sc.longAccumulator("counter");
    //load textfile into spark
    val TextFile = sc.textFile(args(0));
      var maxValue: Int = Integer.parseInt(args(2));
  
  //filter the data as per maxValue              
    val filteredData = TextFile
                    .filter(record => Integer.parseInt(record.split(",")(0)) < maxValue && Integer.parseInt(record.split(",")(1)) < maxValue)
                  
   val broadCaster = filteredData.map(record => (record.split(",")(1), record.split(",")(0)))
                    .groupByKey()
                    .map((record) => (record._1, record._2.toList)) //created a todata broadcaster
                    .collectAsMap();
                    
   val inputData = filteredData.
                   map(record => (record.split(",")(0), record.split(",")(1)))
   inputData.sparkContext.broadcast(broadCaster)
   val newData = inputData.mapPartitions(iter => {
iter.flatMap{
case (k,v1 ) =>
broadCaster.get(k) match {
case None => Seq.empty
case Some(v2) => Seq((k, (v1, v2)))
}
}
}, preservesPartitioning = true)

//filteredData.foreach(println)
    newData.foreach(println)
    val result = newData.map(record =>  record._2._2.map(innerRecord => 
    broadCaster.get(innerRecord) match {
        case Some(l) =>l.contains(record._2._1)
        case None => false } ))
    println(result.collect().flatten.filter(record => record == true).length / 3)
  }
}