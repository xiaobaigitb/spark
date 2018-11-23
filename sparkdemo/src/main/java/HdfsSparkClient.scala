import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext}

/*
  * @Title: HdfsSparkClient
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/15:19:56
  */
object HdfsSparkClient {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("core")

    val sc=new SparkContext(sparkConf)
    import org.apache.hadoop.conf.Configuration
    import com.google.common.io.Resources
    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path

    val con = new Configuration()
    con.addResource(Resources.getResource("core-site-master2.xml"))
    val fileSystem = FileSystem.get(con)
    //val flag = fileSystem.delete(new Path("/sparkwcout"), true);

    val fileName=new Path("/sparkwcout")
    if (fileSystem.exists(fileName)){
      System.out.println(fileName+"已经存在，正在删除它...")
      val flag = fileSystem.delete(fileName, true)
      if (flag){
        System.out.println(fileName+"删除成功")
      }else {
        System.out.println(fileName+"删除失败!")
        return
      }
    }
    val rdd=sc.textFile("hdfs://192.168.228.13:9000/wc/")
    rdd.flatMap(line=>line.split(" "))
      .map(x=>(x,1))
      //.reduceByKey(_+_)
      .reduceByKey((a,b)=>a+b)
      .saveAsTextFile("hdfs://192.168.228.13:9000/sparkwcout/")

    //读取文件
    for ( i<- 0 to 2){
      val inputStream = fileSystem.open(new Path("hdfs://192.168.228.13:9000/sparkwcout/part-0000"+i))
      //byte[] buffer = new byte[1024];
      val buffer=new Array[Byte](1024)
      IOUtils.readFully(inputStream,buffer,0,inputStream.available())
      System.out.println(new String(buffer))
    }
  }
}
