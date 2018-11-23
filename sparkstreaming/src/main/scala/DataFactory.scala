import java.io.PrintWriter
import java.net.ServerSocket

/*
  * @Title: DataFactory
  * @ProjectName spark-scala
  * @Description: TODO
  * @author Mr.lu
  * @date 2018/11/19:10:33
  */
/**
  * 用于造数据
  */
object DataFactory {
  def main(args: Array[String]): Unit = {
    createSocketData()
  }

  def createSocketData(): Unit = {
    val server = new ServerSocket(999)
    val client = server.accept()
    val pw = new PrintWriter(client.getOutputStream)
    while (true) {
      Thread sleep 2000
      pw.println("hello")
      println("print hello")
      pw.println("world")
      println("print world")
      pw.flush()
    }
  }

}
