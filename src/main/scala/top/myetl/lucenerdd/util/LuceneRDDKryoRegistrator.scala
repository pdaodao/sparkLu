package top.myetl.lucenerdd.util

import com.twitter.chill.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import top.myetl.lucenerdd.rdd.{LuceneRDD, IndexRDDPartition}

/**
  * Created by pengda on 17/5/12.
  */
class LuceneRDDKryoRegistrator extends KryoRegistrator{
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[IndexRDDPartition])
  }
}

object LuceneRDDKryoRegistrator {
  def registerKryoClasses(conf: SparkConf): SparkConf = {
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[LuceneRDDKryoRegistrator].getName)
      .set("spark.kryo.registrationRequired", "false")
    /* Set the above to true s.t. all classes are registered with Kryo */
  }
}
