import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSource

object MysqlConnectionPool {
  def main(args: Array[String]): Unit = {
    val conn=getConnection()
    println(conn)
  }

  private val dataSource={
    val dataSource = new DruidDataSource()
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    dataSource.setUrl("jdbc:mysql://localhost:3306/test")
    dataSource.setUsername("root")
    dataSource.setPassword("123")
    dataSource
  }


  def getConnection():Connection= dataSource.getConnection

  def close(connection: Connection)={
    connection.close()
  }
}
