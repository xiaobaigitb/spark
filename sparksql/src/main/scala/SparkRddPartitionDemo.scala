import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*
  * @Title: SparkRddPartitionDemo
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/17:17:29
  */
/**
  * spark分区：vk格式的数据才能分区--通过shuffle操作才进行分区
  */
object SparkRddPartitionDemo {
  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setMaster("local[*]").setAppName("SparkRddPartitionDemo"))

    val rdd=sc.parallelize(List(1,2,3,676,8,9,8080,9,5))
      rdd.collect()
    println(rdd.getNumPartitions)
    val rdd2=sc.textFile("D:\\test\\rootDir\\01.txt")
        .flatMap(line=>line.split(" ")).map(word=>(word,1))
        .reduceByKey(_+_)

    val rdd3=rdd2.partitionBy(new MyPartition(2))
    println(rdd3.partitioner.get)
    rdd3.mapPartitionsWithIndex((index,p)=>{
      p.foreach(e=>println("p:"+index+"e:"+e))
      p
    })

    //partitioner:分区对象
    //采用的也是hash分区=hash（key）/分区数
    //println(rdd.partitioner.get)


    println(rdd2.getNumPartitions)
    println(rdd2.partitioner.get)


  }
}


/**
  * 自定义分区---优点：防止数据倾斜（数据分布不均匀）
  * 1.继承  Partitioner
  * 2.重写numPartitions    getPartition
  * 3.partitionBy：调用
  */
class MyPartition(partitions:Int) extends Partitioner{
  override def numPartitions: Int = partitions

  //1  2：表示的是分区编号
  override def getPartition(key: Any): Int = if (key.toString.startsWith("h")) 1 else 2

}
