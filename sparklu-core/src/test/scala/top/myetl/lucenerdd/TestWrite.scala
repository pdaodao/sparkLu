package top.myetl.lucenerdd

import org.apache.lucene.document.{Document, Field, StringField}
import org.apache.spark.{SparkConf, SparkContext}
import top.myetl.lucenerdd.convert.BeanToDoc
import top.myetl.lucenerdd.util.Constants

/**
  * Created by pengda on 17/1/6.
  */
object TestWrite {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Write to Lucene")
    // 设置hdfs中存储索引的地址 需要在该目录下需要读写权限
    conf.set(Constants.HdfsBaseDirKey, "hdfs://ubuntu:9000/sparklu/")
    // spark上下文
    val sc = new SparkContext(conf)

    // 需要索引的数据
    val data = Seq("Person1", "Person2", "Person3", "Person4", "Person5", "Person6")
    // 数据到lucene Document的转换
    val convert = new BeanToDoc[String] {
      override def toDoc(t: String): Document =  {
        val doc = new Document
        doc.add(new StringField("name", t, Field.Store.YES))
        doc
      }
    }
    // 数据 rdd
    val rdd = sc.parallelize(data, 3)
    // lucene rdd
    val luceneRdd = rdd.saveToLucene("test")(convert)
    // 启动转换
    val count = luceneRdd.run()

    println("记录数:"+count)

    sc.stop()
  }
}
