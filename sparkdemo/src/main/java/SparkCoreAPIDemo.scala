import org.apache.spark.{SparkConf, SparkContext}

/*
  * @Title: SparkCoreAPIDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/15:10:12
  */
/**
  * spark的核心
  */
object SparkCoreAPIDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("core")

    val sc=new SparkContext(sparkConf)

    val rdd=sc.parallelize(List(1,3,5,78,9))
    println(rdd.getNumPartitions)

    val res=rdd.mapPartitions(
      //partition:表示一个分区的内容（一部分数据），是一个集合
      partition=>partition.map(x=>x+1)
    )
    //如果只有计算，类似下面的map
    rdd.map(x=>{
      x+1
    })
  }
}
