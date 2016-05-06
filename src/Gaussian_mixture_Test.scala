import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bkguo on 2016-04-26.
 */
object Gaussian_mixture_Test {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Gaussian mixture test")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val fileData = "D:/email/email/scripts/" +
      "spark-master/spark-master2015-07-16/" +
      "spark-master/data/mllib/" + "gmm_data.txt"

    println("this is the input file " + fileData)
    val gmm_sc = sc.textFile(fileData)
    gmm_sc.take(100).foreach(println)
    val parsedData = gmm_sc.map(s =>
      Vectors.dense(s.trim.split(' ').map(x => x.toDouble))).cache()

    val gmm = new GaussianMixture().setK(2).run(parsedData)
    gmm.gaussians.foreach(println)

  }
}
