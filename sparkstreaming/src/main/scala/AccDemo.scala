import org.apache.spark.{SparkConf, SparkContext}

/*
  * @Title: AccDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/21:17:34
  */
/**
  * acc:可变的累加器 进程间的累加
  */
object AccDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("as"))
    val accum=sc.longAccumulator("acc")
    sc.parallelize(Array(1,2,3,4)).foreach(x=>accum.add(x))
    println(accum.value)
  }

}
