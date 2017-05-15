package top.myetl.lucenerdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import top.myetl.lucenerdd.util.FsUtils

/**
  * Created by pengda on 17/5/12.
  */
class FsTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach{

  val conf :Configuration = new Configuration()
  val path: String = "hdfs://ubuntu:9000/sparklu/"


  "listStatus" should "listStatus" in{

    val dir = FsUtils.dirName(path, "w1")

    println(dir)

    val fs = FsUtils.get(new Path(path), new Configuration())
    val listStatus = fs.listStatus(new Path(dir))
    val paths = FsUtils.listLuceneDir(fs, new Path(dir))
    paths.foreach(println)
  }

}
