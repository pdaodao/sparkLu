package top.myetl.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.lucene.document.{Document, Field, IntPoint, StringField}
import org.apache.lucene.search.ScoreDoc
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Ignore, Matchers}
import top.myetl.lucenerdd.convert.{BeanToDoc, DocToBean}
import top.myetl.lucenerdd.query.MyQuery
import top.myetl.lucenerdd.util.{Constants, LuceneRDDKryoRegistrator}


/**
  * Created by pengda on 17/1/12.
  */
class LuceneWriteTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {



  override def beforeAll(): Unit = {
    conf.set(Constants.HdfsBaseDirKey, "hdfs://ubuntu:9000/sparklu/")
    conf.setAppName("test2app")

    LuceneRDDKryoRegistrator.registerKryoClasses(conf)
    super.beforeAll()
  }


  "write record to lucene " should "write lucene index file to hdfs" in {

    val data = Seq("Person1", "Person2", "Person3", "Person4", "Person5", "Person6")

    val convert = new BeanToDoc[String] {
      override def toDoc(t: String): Document =  {
        val doc = new Document
        doc.add(new StringField("name", t, Field.Store.YES))
        doc
      }
    }
    val rdd = sc.parallelize(data, 3)

    val luceneRdd = rdd.saveToLucene("w1")(convert)
    println("run ........"+luceneRdd.run())
  }

  "query" should "query from lucene index" in {

    val convert = new DocToBean[String] {
      override def toBean(score: ScoreDoc, doc: Document): String = {
        println("--------------- to bean -----------")
        doc.get("name")
      }
    }

    val rdd = sc.luceneRDD("w1")(convert)
    val rdd2 = rdd.query(MyQuery.term("name", "Person1"))
    println("---------------")
    println("-------- count ------"+rdd2.count())
  }





}
