package top.myetl.lucenerdd.convert

import org.apache.lucene.document.Document
import org.apache.lucene.search.ScoreDoc
import scala.reflect.ClassTag

/**
  * When query record from Lucene index
  * Lucene Document to scala bean
  */
abstract class DocToBean[U: ClassTag] extends Serializable{

  /** When read
    * Lucene Document to scala bean
    * @param score
    * @param doc
    * @return
    */
  def toBean(score: ScoreDoc, doc: Document): U

}

/**
  * Write record to Lucene index
  * @tparam T
  */
abstract class BeanToDoc[T: ClassTag] extends Serializable{

  /**
    * When write
    * scala bean to Lucene Document
    * @param t
    * @return
    */
  def toDoc(t: T): Document
}

object LuceneDocConvert{
  import scala.collection.JavaConverters._

  val GetFields  = new DocToBean[Seq[String]] {
    override def toBean(score: ScoreDoc, doc: Document): Seq[String] = {
      doc.getFields.asScala.map(t => t.name)
    }
  }

  val GetDocId = new DocToBean[Int] {
    override def toBean(score: ScoreDoc, doc: Document): Int = score.doc
  }

}
