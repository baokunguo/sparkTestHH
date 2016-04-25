/**
 * Created by bkguo on 2016-04-13.
 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler,OneHotEncoder}
import org.apache.spark.ml.Pipeline

object multi_cols_fea_stackoverflow {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sampledata")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.parquet("s3n://map2-test/forecaster/intermediate_data")

    val df = data.select("win","bid_price","domain","size", "form_factor").na.drop()


    //indexing columns
    val stringColumns = Array("domain","size", "form_factor")
    val index_transformers: Array[org.apache.spark.ml.PipelineStage] = stringColumns.map(
      cname => new StringIndexer()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_index")
    )

    // Add the rest of your pipeline like VectorAssembler and algorithm
    val index_pipeline = new Pipeline().setStages(index_transformers)
    val index_model = index_pipeline.fit(df)
    val df_indexed = index_model.transform(df)


    //encoding columns
    val indexColumns  = df_indexed.columns.filter(x => x contains "index")
    val one_hot_encoders: Array[org.apache.spark.ml.PipelineStage] = indexColumns.map(
      cname => new OneHotEncoder()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_vec")
    )
  }
}