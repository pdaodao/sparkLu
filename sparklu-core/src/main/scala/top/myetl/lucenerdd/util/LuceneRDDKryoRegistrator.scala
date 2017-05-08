package top.myetl.lucenerdd.util

import com.twitter.chill.Kryo
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoRegistrator, KryoSerializer}
import top.myetl.lucenerdd.convert.{BeanToDoc, DocToBean}
import top.myetl.lucenerdd.rdd.{LuceneRDD, LuceneRDDPartition}


/**
  * Created by pengda on 17/1/10.
  */
class LuceneRDDKryoRegistrator extends KryoRegistrator{
  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[LuceneRDD[_]])
    kryo.register(classOf[LuceneRDDPartition[_]])
    kryo.register(classOf[DocToBean[_]])
    kryo.register(classOf[BeanToDoc[_]])
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
