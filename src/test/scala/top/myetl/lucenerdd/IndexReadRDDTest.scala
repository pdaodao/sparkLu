package top.myetl.lucenerdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import top.myetl.lucenerdd.rdd.IndexReadRDD
import top.myetl.lucenerdd.util.{Constants, LuceneRDDKryoRegistrator}

/**
  * Created by pengda on 17/5/12.
  */
class IndexReadRDDTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with SharedSparkContext {


  override def beforeAll(): Unit = {
    conf.set(Constants.HdfsBaseDirKey, "hdfs://ubuntu:9000/sparklu/")
    conf.setAppName("test2app")
    LuceneRDDKryoRegistrator.registerKryoClasses(conf)
    super.beforeAll()
  }

  "IndexReadRDD partitions " should "test new IndexReadRDD" in{
    val rdd = new IndexReadRDD(sc, "w1")
    rdd.cache()
    println(rdd.count())
    println(rdd.count())
  }

}
