import org.apache.spark.ml.{PipelineStage, Pipeline}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.control._
import org.apache.spark.sql.functions._

/**
 * Created by bkguo on 2016-04-19.
 */
object fea_searchcond {
  def main(args: Array[String]) {
    println("Hello")
    val conf = new SparkConf().setAppName("searchCondtion")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val searchCond = sc.textFile("D:/Data/searchCond_sample.csv")
    val searchCondCols = "d,clientcode,starttime,vid,sid,pvid," +
      "city,keyword,hotelstar,price,htlbrand1,htlbrand2,hoteltype," +
      "hotellocation,hotelzone,hotelmark,address,longitude,latitude," +
      "checkin,checkout,isprepayable,isconf,bookable,dist,fac,cmtstype," +
      "cmtslevel,sta,metro1,metro2,municipality,sort_sdk,hotelnum," +
      "emptyhotelnum,bedtype,breakfast,paytype,hotd1"

    val searchCondSchema = new StructType(
    searchCondCols.split(",").map(fieldname => StructField(fieldname,StringType,true))
    )
    val searchCondParse = searchCond.map {
        x =>
        val xlist = x.split(",")
        //Row.fromSeq(xlist.toSeq)
        var seqval = Seq[String]()
        for (i <- xlist) {seqval = seqval:+i}
        Row.fromSeq(seqval)
    }
    //searchCondParse.take(10).foreach(println)
    //searchCondParse.map(x => x.length).take(10).foreach(println)
    val seachCondForm = searchCondParse.filter(x=> x.length == 39)
    //seachCondForm.map(x => x.length).take(100).foreach(println)
    //searchCondParse.map(x => x.length).take(10).foreach(println)
    //System.exit(0)
    val searchCondDF = sqlContext.createDataFrame(seachCondForm,searchCondSchema)
    //searchCondDF.show()
    //searchCondDF.printSchema()

    def isdigitString(x:String):Boolean = {
      var result = true
      val loop = new Breaks
      loop.breakable {
        for (i <- x) {
          if(!i.isDigit){
            result = false
            loop.break
          }
      }
      }
      result
    }
    val toDouble = udf[Double,String](x => if(isdigitString(x)) x.toDouble else Double.NaN)
    val toInt = udf[Int,String](x => if(isdigitString(x)) x.toInt else Int.MaxValue)
    val searchCondDForm = searchCondDF.withColumn("priceNew",toDouble(searchCondDF("price")))
    .withColumn("vidNew",toInt(searchCondDF("vid")))
    .withColumn("sidNew",toInt(searchCondDF("sid")))
    .withColumn("ltNew",toDouble(searchCondDF("longitude")))
    .withColumn("laNew",toDouble(searchCondDF("latitude")))
    .drop("longitude")
    .drop("latitude")
    .drop("sid")
    .drop("vid")
    .drop("price")
    //searchCondDForm.printSchema()
    //searchCondDForm.take(10).foreach(println)
    //val colHeader = searchCondDForm.columns.mkString(",")
    //println(colHeader)
    val fea_transform:Array[PipelineStage] = searchCondDForm.columns.map {
      colname => new OneHotEncoder()
        .setInputCol(colname)
        .setOutputCol(s"${colname}_code")
    }
    //searchCondDForm.dtypes.foreach(println)
    //System.exit(0)
    val fea_index:Array[PipelineStage] = searchCondDForm.columns.map {
      colname => new StringIndexer()
        .setInputCol(colname)
        .setOutputCol(s"${colname}_index")
    }
    val fea_stage = fea_transform ++ fea_index
    val fea_pipe = new Pipeline().setStages(fea_index)
    val fea_model = fea_pipe.fit(searchCondDForm)
    val fea_result = fea_model.transform(searchCondDForm)
    val fea_heads = fea_result.columns.filter(x => x.contains("_"))
    var fea_seq = Seq[String]()
    for (i <- fea_heads) fea_seq = fea_seq :+ i
    println(fea_seq)
    val fea_DF = fea_result.select(fea_seq.head, fea_seq.tail:_*)
    //fea_DF.show()
    fea_DF.rdd.take(10).foreach(println)
    //val fea_input =
    val fea_Double = fea_DF.rdd.map {
      rowRdd =>
        var rowRdd_Double = Seq[Double]()
        for (i <- rowRdd.toSeq) {
          rowRdd_Double = rowRdd_Double :+ i.toString.trim.toDouble
        }
        //rowRdd_Double
        Vectors.dense(rowRdd_Double.toArray)
    }
    //fea_DF.rdd.map(x => Array(x.toSeq)).take(10).foreach(println)
    //fea_Double.take(5).foreach(println)
    val numClusters = 50
    val numIterations = 50
    val clusters = KMeans.train(fea_Double, numClusters, numIterations)
    val WSSSE = clusters.computeCost(fea_Double)
    val fea_pred = clusters.predict(fea_Double)
    //fea_pred.foreach(println)
    val fea_dp = fea_Double.zip(fea_pred)

    fea_dp.foreach(println)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    System.exit(0)
  }

}
