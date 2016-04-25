import breeze.linalg.{DenseVector, DenseMatrix}
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
 * Created by bkguo on 2015-09-06.
 */
object DataType_Test {
  def main(args: Array[String]) {
    val dm = Matrices.dense(3,2,Array(1.0,3.0, 5.0, 2.0, 4.0, 6.0))
    val x = DenseMatrix.zeros[Double](5,5)
    val vect = DenseVector.rand(4)
    println(vect)
  }
}
