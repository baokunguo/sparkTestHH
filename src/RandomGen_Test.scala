import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bkguo on 2015-09-01.
 */

object RandomGen_Test{
  def main(args:Array[String]): Unit = {

    /*config and init*/
    val conf = new SparkConf()
      .setAppName("Spark Pi")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numExamples = 10000 // number of examples to generate
    val fraction = 0.1 // fraction of data to sample

    println("numExample is " + numExamples + " fraction is " + fraction)

    // Example: RandomRDDs.normalRDD
    val normalRDD: RDD[Double] = RandomRDDs.normalRDD(sc, numExamples)
    println(s"Generated RDD of ${normalRDD.count()}" +
      " examples sampled from the standard normal distribution")
    println("  First 10 samples:")
    println(s"this is the normal distribution element ")
    normalRDD.take(10).foreach( x => println(s"    $x") )

    println(" normal vector RDD sample ")
    val normalVectorRDD = RandomRDDs.normalVectorRDD(sc, numRows = numExamples, numCols = 2)
    normalVectorRDD.take(10).foreach(x => println(s"  $x " + " "))

  }
}