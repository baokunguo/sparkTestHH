import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler

/**
 * Created by bkguo on 2016-04-15.
 */
object multi_col_vectorassemble {
  def main(args: Array[String]) {

    val featureHeaders = "clientcode,traceid,hotelid,avg_price," +
      "fq_price,fh_price,dist_htl,isprom,isgift," +
      "iscoupon,isgroup,ismobshare,isreturn,tags_htl"
    //val featureHeaders = featureHeader.collect.mkString(" ")
    //convert the header RDD into a string
    val featureArray = featureHeaders.split(",").toArray

    val quote = "\""
    val featureSIArray = featureArray.map(x => (s"$quote$x$quote"))
    //count the element in headers
    val featureHeader_cnt = featureHeaders.split(",").toList.length
    //featureSIArray.foreach(println)
    //System.exit(0)

    // Fit on whole dataset to include all labels in index.
    import org.apache.spark.ml.feature.StringIndexer
    val labelIndexer = new StringIndexer().
      setInputCol("isreturn").
      setOutputCol("indexedLabel")

    val featureAssembler = new VectorAssembler().
      setInputCols(featureSIArray).
      setOutputCol("features")


    val convpipeline = new Pipeline().
      setStages(Array(labelIndexer, featureAssembler))

    //val myFeatureTransfer = convpipeline.fit(df)
  }
}
