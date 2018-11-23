/*
  * @Title: SparkSqlRDD
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/17:11:31
  */
/**
  * 文本文件的数据源
  * 读取文件-->预处理(变成rdd)-->添加模式--->视图展示-->sql（计算）-->保存
  */
object SparkSqlRDD {
  //模式类：pojo
  case class Word(word:String)
  case class wordcount(word:String)
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    //构建者模式
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    //现将文本文件一行，变成一列
    val rdd=spark.sparkContext
      .textFile("D:\\test\\rootDir\\01.txt")
      .flatMap(line=>line.split(" "))
    rdd.collect().foreach(println)
    /**
      * 加模式的两种方式
      * 需要导入隐士转换
      * 直接指定模式 toDF
      * 通过模式类
      */
    //通过隐士类
    import spark.implicits._
    val df=rdd.toDF("word")

    //2.通过模式类（要写在main外面）todf--推荐使用
    val df2=rdd.map(word=>wordcount(word)).toDF()



    val df3=rdd.map(word=>wordcount(word)).toDS()
    //df3.map(word=>word.)

    df.show()
    df2.show()

    df.createOrReplaceTempView("wc")
    spark.sql("select word,count(word) wordcount from wc group by word")//.show()
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "wc")
      .option("user", "root")
      .option("password", "123")
      .save()
  }

}
