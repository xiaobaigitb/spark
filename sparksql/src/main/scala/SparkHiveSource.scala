/*
  * @Title: SparkHiveSource
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/17:14:07
  */
object SparkHiveSource {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", "hdfs://master2:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    // Queries are expressed in HiveQL

    sql("SELECT * FROM test.stuspark").show()


  }
}
