println("Hello")

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.control.Breaks

/*
import scala.collection.mutable.ArrayBuffer

/*val df = Seq(("a",2),
  ("b",1),("c",0)).toDF("x","y")
*/
val a = ArrayBuffer(1,2,3)
val b = (2,3,4)
a.+=:(122)
*/

val searchCondCols = "d,clientcode,starttime,vid,sid,pvid," +
  "city,keyword,hotelstar,price,htlbrand1,htlbrand2,hoteltype," +
  "hotellocation,hotelzone,hotelmark,address,longitude,latitude," +
  "checkin,checkout,isprepayable,isconf,bookable,dist,fac,cmtstype," +
  "cmtslevel,sta,metro1,metro2,municipality,sort_sdk,hotelnum," +
  "emptyhotelnum,bedtype,breakfast,paytype,hotd1"

searchCondCols.split(",").length

val testval = "2016-02-07,12001103410013947876,2016-02-07 01:00:06.038,17371BDC0C3B49A8883ED7D998333F52,81,994,376,null,五星/豪华,不限,null,null,0,nolimit,null,nolimit,江西省南昌市东湖区苏圃路158号,115.897482,28.682735,20160207,20160208,null,null,null,不限,null,null,null,nolimit,nolimit,null,nolimt,0,26,null,null,null,null,nolimit"

testval.split(",").length

val tlist = testval.split(",")

var tseq = Seq[String]()

for (i <- tlist){tseq = tseq :+ i}
val result = Row.fromSeq(tseq)
Row(tseq.apply(1),tseq.apply(2))


