import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bkguo on 2015-09-07.
 */
object Random2_Test {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Spark Pi")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val nRDD = RandomRDDs.normalRDD(sc,10000L,10)
    //nRDD.take(10).foreach( line => println("nRDD is " + line))

    val pcaPath:String = "hdfs://ns/user/hotel/bkguo/PCA/2015-10-26/download/all/"

    val pcaRdd = sc.textFile(pcaPath)
    val pcaList = pcaRdd.map(x => x.split("\t"))
      .map(xlist => xlist.indexOf(1.0))


    sc.stop()
    System.exit(0)

  }
}
