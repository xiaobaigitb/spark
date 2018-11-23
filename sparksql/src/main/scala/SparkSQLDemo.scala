/*
  * @Title: SparkSQLDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/17:9:56
  */
/**
  * rdd+schema=DataSet
  */
object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    //构建者模式
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    /**
      * implicits:导入隐士转化
      */
    import spark.implicits._
    val df=spark.read.
      json("C:\\soft\\resource\\spark\\spark-2.3.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")

    //通过API操作DataFrame
    df.map(row=>row.getAs[Int]("age")>18)

    //rdd[String]
    //dataframe=rdd[row]
    //Person:自定义的对象
    //dataset=rdd[Person]

    //将DataFrame注册为SQL临时视图--people(视图名就是表名)
    df.createOrReplaceTempView("people")

    //val sqlDF = spark.sql("SELECT count(*) FROM people WHERE age>18")
    //sqlDF.show()

    // 将DataFrame注册为全局临时视图
    df.createGlobalTempView("people")
    // 全局临时视图绑定到系统保留的数据库`global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()

    //全局临时视图是跨会话
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    //df.show()

  }
}
