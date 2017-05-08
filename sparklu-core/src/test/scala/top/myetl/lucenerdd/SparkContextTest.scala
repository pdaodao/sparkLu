package top.myetl.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.lucene.document.Document
import org.apache.lucene.search.ScoreDoc
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Ignore, Matchers}
import top.myetl.lucenerdd.convert.DocToBean
import top.myetl.lucenerdd.util.{Constants, LuceneRDDKryoRegistrator}
import top.myetl.lucenerdd.query.MyQuery._
import top.myetl.lucenerdd.rdd.LuceneRDD

/**
  * Created by pengda on 17/1/5.
  */
@Ignore
class SparkContextTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {

  var rdd : LuceneRDD[_] = _

  override def beforeAll(): Unit = {
    conf.set(Constants.HdfsBaseDirKey, "hdfs://ubuntu:9000/sparklu/")
    conf.setAppName("test1app")
    LuceneRDDKryoRegistrator.registerKryoClasses(conf)
    super.beforeAll()

    val convert = new DocToBean[String] {
      override def toBean(score: ScoreDoc, doc: Document): String = score.doc.toString+" -> "+doc.get("_all")
    }
    rdd = sc.luceneRDD("test")(convert)
    rdd.setName("rdd")
  }



  "sparkContext" should "SparkContext functions" in {

    val queryRdd = rdd.query(term("_all", "测试9999"))

    println(queryRdd.take(2).map(println))
    println(rdd.query(should(term("_all","测试9999"), term("_all", "测试12345"))).collect().map(println))


//    queryRdd.take(5).map(println)

//    println("---------------------------------- query result "+queryRdd.query(term("_all", "123")).count())
//    println("---------------------------------- query result "+queryRdd.query(term("_all", "456")).count())
  }

  "get fields" should "show fields" in {
    rdd.fields().map(println)
  }

  "just test" should "sqlContext.implicits" in {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    Seq(1,2,3).toDS()

    InternalRow

  }
}
