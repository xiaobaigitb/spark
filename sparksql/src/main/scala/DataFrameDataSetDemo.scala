/*
  * @Title: DataFrameDataSetDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/17:16:58
  */
object DataFrameDataSetDemo {
  case class CityIp(city:String,ip:String)
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    //构建者模式
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    val rdd=spark.sparkContext
      .textFile("D:\\test\\rootDir\\01.txt")
      .map(line=>line.split(" "))
      .map(arr=>CityIp(arr(0),arr(1)))

    import spark.implicits._
    val df=rdd.toDF()
    df.filter(row=>row.getAs("city").equals("guangzhou")).show()
    val ds=rdd.toDS()
    ds.filter(cityIp=>cityIp.city.equals("guangzhou")).show()

    df.createOrReplaceTempView("cip")
    ds.createOrReplaceTempView("cip2")

    spark.sql("select * from cip where city='guangzhou'")
    spark.sql("select * from cip2 where city='guangzhou'")

  }
}
