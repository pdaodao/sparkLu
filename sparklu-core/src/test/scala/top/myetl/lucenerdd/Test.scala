package top.myetl.lucenerdd

import org.apache.spark.sql.DataFrameHolder

/**
  * Created by pengda on 17/1/6.
  */
object Test {

  def main(args: Array[String]): Unit = {

    Map("a"->1, "b"->2).groupBy( t => t._1)

    DataFrameHolder
  }

}
