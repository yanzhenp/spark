```
/**
  * Created by yanzhenp on 12/13/2016.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.log4j.{Level, Logger}

object transformation {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\Program Files\\hadoop-2.7.3");
    //val logFile = "C:/update.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    //val logData = sc.textFile(logFile, 2).cache()
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    val distData1 = sc.parallelize(1 to 10)
    print("map(func):")
    distData1.map(_*2).collect().foreach(println)
    print("filter(func):")
    distData1.filter(x => x >= 5).collect().foreach(println)
    print("flatmap(func):")
    distData1.flatMap(x => List(x, 1)).collect().foreach(println)

    print("union(otherDataset):")
    val distData2 = sc.makeRDD(1 to 3)
    (distData1 union distData2).collect().foreach(println)

    print("intersection(otherDataset):")
    (distData1.intersection(distData2)).collect().foreach(println)

    print("distinct([num Tasks]):")
    (distData1 union distData2).distinct().collect().foreach(println)

    print("groupByKey([num Tasks]):")
    val distData3 = sc.makeRDD(List((1,2),(1,3),(1,4),(2,3),(2,5),(3,6)))
    distData3.groupByKey().collect().foreach(println)

    print("reduceByKey(func, [num Tasks]):")
    distData3.reduceByKey(_+_).collect().foreach(println)

    print("aggregateByKey(zeroValue)(seqOp,combOp,[num Tasks]):")
    def seqOp(a: Int, b: Int): Int = {
      return Math.max(a,b)
    }
    def combOp(a: Int, b: Int): Int = {
      return a + b
    }
    distData3.aggregateByKey(3)(seqOp, combOp).collect().foreach(println)
    // key = 1: 2,3,4
    // key = 2: 3,4
    // key = 3: 6
    // result should be: (1,7),(2,8),(3,6)
    // note: output in Intellij IDEA is different with spark-shell's.

    print("sortByKey([ascending],[num Tasks]):")
    distData3.sortByKey(false).collect().foreach(println)

    print("join(otherDataset, [num Tasks]):")
    val distData4 = sc.makeRDD(List((1,8),(1,13),(2,4),(5,7)))
    (distData3.join(distData4)).collect().foreach(println)
  }
}
```
