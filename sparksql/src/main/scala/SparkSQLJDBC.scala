/*
  * @Title: SparkSQLJDBC
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/17:11:20
  */
/**
  * spark操作数据库数据
  */
object SparkSQLJDBC {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    //构建者模式
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    /**
      * 加载数据库中的表
      */
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "teacher")
      .option("user", "root")
      .option("password", "123")
      .load()


    /**
      * 往数据库中写入数据
      */

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "teacher2")
      .option("user", "root")
      .option("password", "123")
      .save()

    //jdbcDF.show()
  }
}
