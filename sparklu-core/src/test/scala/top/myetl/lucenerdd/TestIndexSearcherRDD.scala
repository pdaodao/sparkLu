package top.myetl.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.lucene.document.Document
import org.apache.lucene.search.{IndexSearcher, ScoreDoc}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import top.myetl.lucenerdd.convert.DocToBean
import top.myetl.lucenerdd.query.MyQuery.term
import top.myetl.lucenerdd.rdd.{LuceneQueryRDD, LuceneSearcherRDD}
import top.myetl.lucenerdd.util.{Constants, LuceneRDDKryoRegistrator}

/**
  * Created by pengda on 17/5/8.
  */
class TestIndexSearcherRDD extends FlatSpec
with Matchers
with BeforeAndAfterEach
with SharedSparkContext {

  var rdd: LuceneSearcherRDD = _

  override def beforeAll(): Unit = {
    conf.set(Constants.HdfsBaseDirKey, "hdfs://ubuntu:9000/sparklu/")
    conf.setAppName("test1app")
    LuceneRDDKryoRegistrator.registerKryoClasses(conf)
    super.beforeAll()

    rdd = new LuceneSearcherRDD(sc, "w1")
    rdd.setName("rdd")
  }

  "get partitions " should "get partitions" in {

    rdd.cache()

//    println("1 -> "+rdd.count())
//    println("-------------------")
//    println("2 -> "+rdd.count())

    val convert = new DocToBean[String] {
      override def toBean(score: ScoreDoc, doc: Document): String = score.doc.toString+" -> "+doc.get("name")
    }

    val newRDD = new LuceneQueryRDD[String](rdd)(convert)

    newRDD.query(term("name", "Person2")).take(2).foreach(println(_))

    val newRDD2 = new LuceneQueryRDD[String](rdd)(convert)

    newRDD2.query(term("name", "Person1")).take(2).foreach(println(_))



  }

}