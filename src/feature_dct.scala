import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row

/**
 * Created by bkguo on 2016-04-11.
 */
object feature_dct {
  def main (args: Array[String]) {
    println("HelloWorld")

    val conf = new SparkConf()
    .setAppName("feature_dct")
    .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val sampledata = sc.textFile("D:/Data/sample_data.txt")

    //val schemaString = "clientcode\tvid\tsid\tpvid\tstarttime\ttraceid\tscreenheight\tnetworktype\tcarrier\tcity\tcountryname\tdevicename\tiswechatwakeup\tdevicetype\tos\tosversion\tuid\tdate_d\tip\tdestination\tkeyword\tcityid\tprice\tstar\taddress\tlongitude\tlatitude\tcheckin\tcheckout\tisprepayable\tisconf\tbookable\tspec\tbrand1\tbrand2\tbrand3\tdist\tcmtstype\tcmtslevel\tfilter_score\tfilter_quantity\tcom\tsta\tdistr\tmetro1\tmetro2\thotd\tmunicipality\tsort_v\thotelid\tavg_price\tfq_price\tfh_price\tdist_htl\tisprom\tisgift\tiscoupon\tisgroup\tismobshare\tisreturn\ttags_htl"
    val schemaString = "clientcode\tvid\tsid\tpvid\tstarttime\ttraceid\tscreenheight\tnetworktype\tcarrier\tcity"
    val schema = new StructType(
      schemaString.split("\t").map(fieldName => StructField(fieldName,StringType,true)))

    //println(schema)
    //val ss = schemaString.split("\t").zipWithIndex
    //print(ss(0))
    //System.exit(0)
    val parseSample = sampledata.map { line =>
      val linelist = line.split(",")
      Row(linelist(0),linelist(1),linelist(2),linelist(3),linelist(4)
      ,linelist(5),linelist(6),linelist(7),linelist(8),linelist(9))
    }
    //parseSample.take(10).foreach(println)
    val sampSchema = sqlContext.createDataFrame(parseSample,schema)
    //sqlContext.creat
    //sampSchema.take(10).foreach(println)
    //System.exit(0)
    sampSchema.registerTempTable("sampleTable")

    val result = sqlContext.sql("select clientcode, networktype, carrier, city from sampleTable")
    result.show()
    //System.exit(0)
    /*
    val indexer = new StringIndexer()
      .setInputCol("carrier")
      .setOutputCol("carrierIndex")
      .fit(result)

    val indexed = indexer.transform(result)
    indexed.show()

    val encoder = new OneHotEncoder()
      .setInputCol("carrierIndex")
      .setOutputCol("carrierVec")
    val encoded = encoder.transform(indexed)
    */
    //encoded.select("carrier", "carrierIndex", "carrierVec").show()
    //encoded.select("carrierVec").foreach(println)

    val stringColumns = Array("networktype","carrier", "city")

    val index_transformers: Array[org.apache.spark.ml.PipelineStage] = stringColumns.map(
      cname => new StringIndexer()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_index"))

    val index_pipeline = new Pipeline().setStages(index_transformers)
    val index_model = index_pipeline.fit(result)
    val result_indexed = index_model.transform(result)
    result_indexed.show()
  }

}
/*

import org.apache.spark.ml.feature.VectorIndexer

val data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

val indexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexed")
  .setMaxCategories(10)

val indexerModel = indexer.fit(data)

val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
println(s"Chose ${categoricalFeatures.size} categorical features: " +
  categoricalFeatures.mkString(", "))

// Create new column "indexed" with categorical values transformed to indices
val indexedData = indexerModel.transform(data)
indexedData.show()

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}

val df = sqlContext.createDataFrame(Seq(
  (0, "a"),
  (1, "b"),
  (2, "c"),
  (3, "a"),
  (4, "a"),
  (5, "c")
)).toDF("id", "category")

val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex")
  .fit(df)
val indexed = indexer.transform(df)

val encoder = new OneHotEncoder()
  .setInputCol("categoryIndex")
  .setOutputCol("categoryVec")
val encoded = encoder.transform(indexed)
encoded.select("id", "categoryVec").show()

case class sampleFormat(name: String, age: Int
                        ,clientcode: String
                        ,vid: String
                        ,sid: Int
                        ,pvid: Int
                        ,starttime: String
                        ,traceid: String
                        ,screenheight: String
                        ,networktype: String
                        ,carrier: String
                        ,city: String
                        ,countryname: String
                        ,devicename: String
                        ,iswechatwakeup: String
                        ,devicetype: String
                        ,os: String
                        ,osversion: String
                        ,uid: String
                        ,date_d: String
                        ,ip: String
                        ,destination: String
                        ,keyword: String
                        ,cityid: String
                        ,price: String
                        ,star: String
                        ,address: String
                        ,longitude: String
                        ,latitude: String
                        ,checkin: String
                        ,checkout: String
                        ,isprepayable: String
                        ,isconf: String
                        ,bookable: String
                        ,spec: String
                        ,brand1: String
                        ,brand2: String
                        ,brand3: String
                        ,dist: String
                        ,cmtstype: String
                        ,cmtslevel: String
                        ,filter_score: String
                        ,filter_quantity: String
                        ,com: String
                        ,sta: String
                        ,distr: String
                        ,metro1: String
                        ,metro2: String
                        ,hotd: String
                        ,municipality: String
                        ,sort_v: String
                        ,hotelid: String
                        ,avg_price: String
                        ,fq_price: String
                        ,fh_price: String
                        ,dist_htl: String
                        ,isprom: String
                        ,isgift: String
                        ,iscoupon: String
                        ,isgroup: String
                        ,ismobshare: String
                        ,isreturn: String
                        ,tags_htl: String)
*/