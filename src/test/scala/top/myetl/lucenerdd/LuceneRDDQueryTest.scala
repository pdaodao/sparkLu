package top.myetl.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.lucene.document.Document
import org.apache.lucene.search.ScoreDoc
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import top.myetl.lucenerdd.convert.DocToBean
import top.myetl.lucenerdd.query.MyQuery
import top.myetl.lucenerdd.query.MyQuery.term
import top.myetl.lucenerdd.rdd.{IndexReadRDD, LuceneRDD}
import top.myetl.lucenerdd.util.{Constants, LuceneRDDKryoRegistrator}

/**
  * Created by pengda on 17/5/12.
  */
class LuceneRDDQueryTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {


  override def beforeAll(): Unit = {
    conf.set(Constants.HdfsBaseDirKey, "hdfs://ubuntu:9000/sparklu/")
    conf.setAppName("test2app")
    LuceneRDDKryoRegistrator.registerKryoClasses(conf)
    super.beforeAll()
  }

  val convert = new DocToBean[String] {
    override def toBean(score: ScoreDoc, doc: Document): String = score.doc.toString+" -> "+doc.get("name")
  }

  "Query by step" should "query by step" in{
    val rdd = new IndexReadRDD(sc, "w1")
    rdd.cache()
    println(rdd.count())
    println(rdd.count())

    val newRDD = new LuceneRDD[String](rdd)(convert)

    newRDD.query(term("name", "Person2")).take(8).foreach(println(_))

  }

  "Simple" should "simple api for query" in{
    val rdd: LuceneRDD[String] = sc.luceneRDD("w1")(convert)
    println(rdd.count())

    rdd.query(MyQuery.matchAll).take(8).foreach(println(_))
  }

}
