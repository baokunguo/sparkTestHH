import org.apache.spark.ml.{PipelineModel, PipelineStage, Pipeline}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by bkguo on 2016-04-13.
 */
object feature_query_dct {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("feature_dct")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val sampledata = sc.textFile("D:/firefoxDownload/sample_data.txt")
    val schemaString = "clientcode\tvid\tsid\tpvid\tstarttime\ttraceid\t" +
      "screenheight\tnetworktype\tcarrier\tcity\tcountryname\tdevicename\t" +
      "iswechatwakeup\tdevicetype\tos\tosversion\tuid\tdate_d\tip\t" +
      "destination\tkeyword\tcityid\tprice\tstar\taddress\tlongitude\t" +
      "latitude\tcheckin\tcheckout\tisprepayable\tisconf\t" +
      "bookable\tspec\tbrand1\tbrand2\tbrand3\tdist\tcmtstype\t" +
      "cmtslevel\tfilter_score\tfilter_quantity\tcom\tsta\tdistr\t" +
      "metro1\tmetro2\thotd\tmunicipality\tsort_v\thotelid\tavg_price\t" +
      "fq_price\tfh_price\tdist_htl\tisprom\tisgift\tiscoupon\tisgroup\t" +
      "ismobshare\tisreturn\ttags_htl"

    val sub_schemaString = "clientcode\ttraceid\thotelid\tavg_price\t" +
      "fq_price\tfh_price\tdist_htl\tisprom\tisgift\tiscoupon\tisgroup\t" +
      "ismobshare\tisreturn\ttags_htl"


    val schema = new StructType(
      sub_schemaString.split("\t").map(fieldName => StructField(fieldName,StringType,true)))

    val parseSample = sampledata.map { line =>
      val linelist = line.split(",")
      Row(linelist(0),linelist(5),linelist(49),linelist(50),linelist(51)
        ,linelist(52),linelist(53),linelist(54),linelist(55),linelist(56)
        ,linelist(57),linelist(58),linelist(59),linelist(60))
    }

    val htlsample = sqlContext.createDataFrame(parseSample,schema)
    htlsample.registerTempTable("htl_ui_info")
    htlsample.take(10).foreach(println)

    val htlsample_info = sqlContext.sql("select clientcode, traceid, hotelid, avg_price, " +
      "fq_price, fh_price, dist_htl, isprom, isgift, iscoupon, isgroup, ismobshare, " +
      "isreturn, tags_htl from htl_ui_info")

    htlsample_info.show()
    val stringColumns = Array("isprom","iscoupon", "ismobshare")

    val index_transform:Array[PipelineStage] = stringColumns.map {
      cnames =>
        new StringIndexer()
        .setInputCol(cnames)
        .setOutputCol(s"${cnames}_index")
    }

    val index_pipeline = new Pipeline().setStages(index_transform)
    val index_model = index_pipeline.fit(htlsample)
    val index_result = index_model.transform(htlsample)
    index_result.show()
    index_result.printSchema()
  }
}
