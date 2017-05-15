package top.myetl.lucenerdd.store


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.document.{Document, Field, IntPoint, StringField}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery, TermQuery}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import top.myetl.lucenerdd.util.FsUtils

/**
  * Created by pengda on 17/5/12.
  */
class TestStore extends FlatSpec
  with Matchers
  with BeforeAndAfterEach{

  val conf :Configuration = new Configuration()
  val path: String = "hdfs://ubuntu:9000/sparklu/test1"


  val directory = {
    val dirTimeStart = System.currentTimeMillis();
    val dir = HdfsDirectory(path, conf)
    val dirTimeEnd = System.currentTimeMillis();
    println("new HdfsDirectory time:"+(dirTimeEnd - dirTimeStart));

    dir
  }



  def writeIndexToHdfs(): Unit ={
    val indexTimeStart = System.currentTimeMillis()
    val dir = directory
    val indexWriter = new IndexWriter(dir, new IndexWriterConfig().setOpenMode(OpenMode.CREATE_OR_APPEND))
    val indexTimeEnd = System.currentTimeMillis();
    println("new Index time:"+(indexTimeEnd - indexTimeStart));

    val t1 = System.currentTimeMillis()
    val stepCount = 2
    val stepSize = 500
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
    dir.close()
    val t2 = System.currentTimeMillis()
    println("write lucene index to hdfs: record number:"+stepCount*stepSize+", time:"+(t2 - t1) / 1000 +"s")

    val p = new Path(path)
    val fs = FsUtils.get(p, conf)

    val paths = FsUtils.listLuceneDir(fs, p)
    paths.foreach(println)
  }


  "Write HdfsDirectory" should "write index to hdfs" in {
   writeIndexToHdfs()
  }

  "Read HdfsDirectory" should "query by lucene" in {
    new Thread(new Runnable {
      override def run() = {
        query()
      }
    }).start()

    new Thread(new Runnable {
      override def run() = {
        query()
      }
    }).start()

    Thread.sleep(10000)

  }

  "MultiThread" should "write and read" in{

    new Thread(new Runnable {
      override def run() = {
        query()
      }
    }).start()

    Thread.sleep(50)

    new Thread(new Runnable {
      override def run() = {
        writeIndexToHdfs()
      }
    }).start()

    Thread.sleep(90000)
  }


  def query(): Unit = {
    val dir = directory
    val readerTimeStart = System.currentTimeMillis()
    val indexReader = DirectoryReader.open(directory)
    val readerTimeEnd = System.currentTimeMillis()
    println("new indexReader time:"+(readerTimeEnd - readerTimeStart))

    val indexSearcherTimeStart = System.currentTimeMillis()
    val indexSearcher = new IndexSearcher(indexReader)
    val indexSearcherTimeEnd = System.currentTimeMillis()
    println("new IndexSearcher time:"+(indexSearcherTimeEnd - indexSearcherTimeStart))


    val countTimeStart = System.currentTimeMillis();
    val MatchAllDocs = new MatchAllDocsQuery
    val count = indexSearcher.count(MatchAllDocs)
    val countTimeEnd = System.currentTimeMillis()
    println("doc count:"+count+" time:"+(countTimeEnd - countTimeStart))


    val queryTimeStart = System.currentTimeMillis()
    val IntSearch = Array(3, 100, 9999, 19999, 39999, 53245, 9876523)
    IntSearch.map{ t =>
      val ageQuery =  IntPoint.newExactQuery("age", t)
      indexSearcher.search(ageQuery, 10).scoreDocs.map{doc => print(doc.doc+"  ")}
    }

    IntSearch.map{ t =>
      val v = "测试"+t
      val term = new Term("_all", v)
      val tq = new TermQuery(term)
      indexSearcher.search(tq, 10).scoreDocs.map{ doc => print(doc.doc+"  "+indexSearcher.doc(doc.doc).get("_all")+"  ")}
    }
    val queryTimeEnd = System.currentTimeMillis()
    println("query time:"+(queryTimeEnd - queryTimeStart))

    indexReader.close()
  }

}
