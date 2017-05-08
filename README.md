# sparkLu
使用lucene为spark rdd提供索引功能，索引数据存储到HDFS上，目标是: 带有join功能的分布式搜索引擎


## 创建索引

```scala

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

```

## 查询
```scala
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
```
