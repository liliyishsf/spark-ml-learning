import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}


object Feature_handling_vector {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val data_path = "/home/liliyi/data/spark_data/data1/txt"
    val data = spark.read.text(data_path).map {
      case Row(line: String) =>
        val arr = line.split(',')
        (arr(0), Vectors.dense(arr(1).split(' ').map(_.toDouble)))
    }.toDF("label", "features")
    data.show(false)
//    sc.stop()
  }
}
