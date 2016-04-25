import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
2.2*1024/(3*21)


val schemaString = "clientcode\tvid\tsid\tpvid\tstarttime\ttraceid\tscreenheight\tnetworktype\tcarrier\tcity\tcountryname\tdevicename\tiswechatwakeup\tdevicetype\tos\tosversion\tuid\tdate_d\tip\tdestination\tkeyword\tcityid\tprice\tstar\taddress\tlongitude\tlatitude\tcheckin\tcheckout\tisprepayable\tisconf\tbookable\tspec\tbrand1\tbrand2\tbrand3\tdist\tcmtstype\tcmtslevel\tfilter_score\tfilter_quantity\tcom\tsta\tdistr\tmetro1\tmetro2\thotd\tmunicipality\tsort_v\thotelid\tavg_price\tfq_price\tfh_price\tdist_htl\tisprom\tisgift\tiscoupon\tisgroup\tismobshare\tisreturn\ttags_htl"
val ss = schemaString.split("\t")

val res = Array.range(50,61,1)

res.foreach(x => print(",linelist(" +
  x.toString + ")"))

val sub_schemaString = "clientcode\ttraceid\thotelid\tavg_price\t" +
  "fq_price\tfh_price\tdist_htl\tisprom\tisgift\tiscoupon\tisgroup\t" +
  "ismobshare\tisreturn\ttags_htl"

sub_schemaString.split("\t")

/*
val datapath = "D:/eclipse_workplace/sparkmaster/" +
  "spark-master/spark-master/data/mllib/sample_libsvm_data.txt"

val conf = new SparkConf().setAppName("sampledata")
.setMaster("local")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
*/
//val sdata = sqlContext.read.format("libsvm").load(datapath)
336/25.0

