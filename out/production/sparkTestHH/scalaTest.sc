println(s"Generated RDD of ${123}" +
  " examples sampled from the standard normal distribution")


/*
import scala.io.Source
val source = Source.fromFile("D:/firefoxDownload/dptp_htl_rankfactors.csv","UTF-8")
source.reader()
val lineIterator = source.getLines
lineIterator.next()
lineIterator.next()
Source.stdin
*/

val searchCondCols = "d,clientcode,starttime,vid,sid,pvid," +
  "city,keyword,hotelstar,price,htlbrand1,htlbrand2,hoteltype," +
  "hotellocation,hotelzone,hotelmark,address,longitude,latitude," +
  "checkin,checkout,isprepayable,isconf,bookable,dist,fac,cmtstype," +
  "cmtslevel,sta,metro1,metro2,municipality,sort_sdk,hotelnum," +
  "emptyhotelnum,bedtype,breakfast,paytype,hotd1"

searchCondCols.split(",").length

val testval = "2016-02-07,12001103410013947876,2016-02-07 01:00:06.038,17371BDC0C3B49A8883ED7D998333F52,81,994,376,null,����/����,����,null,null,0,nolimit,null,nolimit,����ʡ�ϲ��ж���������·158��,115.897482,28.682735,20160207,20160208,null,null,null,����,null,null,null,nolimit,nolimit,null,nolimt,0,26,null,null,null,null,nolimit"
testval.split(",").length
val tlist = testval.split(",")
var tseq = Seq[String]()
for (i <- tlist){
  tseq :+ i
}
tseq


/*
org.apache.spark.sql.Row(tlist(1)).get(0)
val rfactory =  org.apache.spark.sql.RowFactory
println("H")
val rfval = rfactory.create(testval.split(",").toSeq)
val testsql = org.apache.spark.sql.Row(testval.split(",").toSeq)
testsql.getSeq(0)
*/
/**
val source1 = Source.fromURI("http://www.baidu.com","UTF-8")
source1.getLines.next()


// ��URL��ȡ����Ҫע���ַ�����
val source1 = Source.fromURL("http://horstmann.com", "UTF-8")
val source2 = Source.fromString("Hello, world!")  // ��ָ�����ַ�����ȡ������ʱ������
val source3 = Source.stdin  // �ӱ�׼�����ȡ

