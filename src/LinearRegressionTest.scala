import breeze.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bkguo on 2015-09-02.
 */
object LinearRegressionTest {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("LinearRegressionTest")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val fileData = "D:/email/email/scripts/" +
      "spark-master/spark-master2015-07-16/" +
      "spark-master/data/mllib/ridge-data/" +
      "lpsa.data"

    println("this is the input file " + fileData)

    val data = sc.textFile(fileData)

    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,
        Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    println(parsedData.count() + " is the data count")
    //parsedData.take(10).foreach(println)

    val numIternation = 100
    val model = LinearRegressionWithSGD.train(parsedData,numIternation)

    println(model.weights.toString())

    val valuesAndPreds = parsedData.map {point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //valuesAndPreds.foreach(println)
    val mse = valuesAndPreds.map {case (v,p) => math.pow((v-p),2)}

    sc.stop()
    System.exit(0)

  }
}