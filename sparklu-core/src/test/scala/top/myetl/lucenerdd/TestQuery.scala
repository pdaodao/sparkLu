package top.myetl.lucenerdd

import org.apache.lucene.document.Document
import org.apache.lucene.search.ScoreDoc
import org.apache.spark.{SparkConf, SparkContext}
import top.myetl.lucenerdd.convert.DocToBean
import top.myetl.lucenerdd.query.MyQuery.term
import top.myetl.lucenerdd.rdd.LuceneRDD
import top.myetl.lucenerdd.util.Constants

/**
  * Created by pengda on 17/1/6.
  */
object TestQuery {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Read from Lucene")
    // 设置hdfs中存储索引的地址 需要在该目录下需要读写权限
    conf.set(Constants.HdfsBaseDirKey, "hdfs://ubuntu:9000/sparklu/")
    // spark上下文
    val sc = new SparkContext(conf)

    // lucene 到 java bean 的转换
    val convert = new DocToBean[String] {
      override def toBean(score: ScoreDoc, doc: Document): String = score.doc.toString+" -> "+doc.get("_all")
    }
    //创建 luceneRDD
    val rdd: LuceneRDD[String] = sc.luceneRDD("test")(convert)
    rdd.setName("rdd")

    val queryRdd = rdd.query(term("name", "Person3"))

    println(queryRdd.take(2).map(println))

    sc.stop()
  }

}
