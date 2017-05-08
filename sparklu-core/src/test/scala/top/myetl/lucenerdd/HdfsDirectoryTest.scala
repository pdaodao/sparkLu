package top.myetl.lucenerdd

import org.apache.hadoop.conf.Configuration
import org.apache.lucene.document.{Document, Field, IntPoint, StringField}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.search._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import top.myetl.lucenerdd.store.HdfsDirectory

/**
  * Created by pengda on 17/1/4.
  */
class HdfsDirectoryTest extends FlatSpec
  with Matchers
  with BeforeAndAfterEach{


  "HdfsDirectory test" should "write file to hdfs" in {

    val conf:Configuration = new Configuration()
    conf.addResource(new org.apache.hadoop.fs.Path("core-site.xml"))
    conf.addResource(new org.apache.hadoop.fs.Path("hdfs-site.xml"))

    val path: String = "hdfs://ubuntu:9000/sparklu/test3"

    // write test data to hdfs as lucene
    Help.write(path, conf)

    // query
//    Help.query(path, conf)

  }


}

object Help{

  /**
    * term query
    * @param path
    * @param conf
    */
  def query(path: String, conf: Configuration): Unit = {
    val t1Open = System.currentTimeMillis()
    val readDirectory = HdfsDirectory(path, conf)
    val indexReader = DirectoryReader.open(readDirectory)
    val indexSearcher = new IndexSearcher(indexReader)
    val t2Open = System.currentTimeMillis()

    println("---------------------------------")
    println("open directory time:"+(t2Open - t1Open) / 1000 +"(s)")


    val t1Count = System.currentTimeMillis()
    val MatchAllDocs = new MatchAllDocsQuery
    val t2Count = System.currentTimeMillis()
    println("doc count:"+indexSearcher.count(MatchAllDocs)+" time:"+(t2Count - t1Count)+"ms")


    val IntSearch = Array(3, 100, 9999, 19999, 39999, 53245, 9876523)

    IntSearch.map{ t =>
      val t1 = System.currentTimeMillis()
      val ageQuery =  IntPoint.newExactQuery("age", t)
      indexSearcher.search(ageQuery, 10).scoreDocs.map{doc => print(doc.doc+"  ")}
      val t2 = System.currentTimeMillis()
      println("  int equal search "+t+"  "+(t2 - t1)+" ms")
    }

    IntSearch.map{ t =>
      val v = "测试"+t
      val term = new Term("_all", v)
      val tq = new TermQuery(term)
      val t1 = System.currentTimeMillis()
      indexSearcher.search(tq, 10).scoreDocs.map{ doc => print(doc.doc+"  "+indexSearcher.doc(doc.doc).get("_all")+"  ")}
      val t2 = System.currentTimeMillis()
      println(" term equal search "+v+"  "+(t2 - t1)+" ms")
    }

  }

  /**
    * Multi Thread query test
    * @param indexSearcher
    */
  def multiThread(indexSearcher: IndexSearcher): Unit = {
    val runners =  1.to(10).map{ _ =>
      val runner = new Runnable {
        override def run() = {
          for(i <- 1 to 10){
            val ageQuery =  IntPoint.newExactQuery("age", 100)
            indexSearcher.search(ageQuery, 10).scoreDocs.map{doc => println(doc.doc+"  ")}
          }
        }
      }
      runner
    }

    runners.foreach(_.run())
  }

  def localWrite(): Unit = {
    // write data to local filesystem
    //    val file = new File("/Users/pengda/spark/test")
    //    val directory = new SimpleFSDirectory(file.toPath)

  }

  /**
    * write test data to HdfsDirectory
    * @param path
    * @param conf
    */
  def write(path: String, conf: Configuration): Unit = {
    val directory = HdfsDirectory(path, conf)
    val indexWriter = new IndexWriter(directory,
      new IndexWriterConfig().setOpenMode(OpenMode.CREATE))
    val t1 = System.currentTimeMillis()
    val stepSize = 500
    val stepCount = 2
    for(j <- 1 to stepCount){
      for(i <- 1 to stepSize){
        val doc = new Document
        doc.add(new StringField("_all", "测试"+i, Field.Store.YES))
        doc.add(new StringField("_name","名称"+i, Field.Store.YES))
        doc.add(new IntPoint("age", i))
        indexWriter.addDocument(doc)
      }
      println("indexWriter ram used(Bytes): "+indexWriter.ramBytesUsed())
      indexWriter.commit()
      println("indexWriter ram used(Bytes): "+indexWriter.ramBytesUsed())
    }
    indexWriter.forceMerge(5)
    indexWriter.close()
    directory.close()
    val t2 = System.currentTimeMillis()
    println("etl data to lucene time "+(t2 - t1) / 1000 +"(s)")
  }

}