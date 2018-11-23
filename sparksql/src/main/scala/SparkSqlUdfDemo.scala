import java.text.SimpleDateFormat

/*
  * @Title: SparkSqlUdfDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/17:17:13
  */
/**
  * 自定义spark函数
  */
object SparkSqlUdfDemo {
  def main(args: Array[String]): Unit = {
    val date="2018-11-17 17:14:11" //-->"2018-11-17 17:14"

    val myudf=(date:String)=>{
      val format="yyyy-MM-dd HH:mm"
      val sdf=new SimpleDateFormat(format)
        sdf.format(sdf.parse(date))
    }

    //println(myudf(date))
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Hive Example")
      .getOrCreate()
    import spark.implicits._
    val df=spark.sparkContext.parallelize(List("2018-11-17 17:14:11")).toDF("mydate")
    df.createOrReplaceTempView("t")
    //spark:注册函数(自定义函数)
    spark.sqlContext.udf.register("myudf",myudf)
    spark.sql("select myudf(mydate) from t").show()
  }
}
