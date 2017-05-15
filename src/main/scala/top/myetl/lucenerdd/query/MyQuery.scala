package top.myetl.lucenerdd.query

import org.apache.lucene.document.{DoublePoint, FloatPoint, IntPoint, LongPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.{BooleanQuery, MatchAllDocsQuery, Query, TermQuery}


sealed trait MyQuery{

  def build(): Query
}

case class MatchAll() extends MyQuery{
  override def build(): Query = new MatchAllDocsQuery
}



/**
  * Term equal query
  * @param name
  * @param value
  */
case class TermEqual(name: String, value: Any) extends MyQuery{
  override def build(): Query = {
    value match {
      case s: String => new TermQuery(new Term(name, s))
      case i: Int => IntPoint.newExactQuery(name, i)
      case l: Long => LongPoint.newExactQuery(name, l)
      case f: Float => FloatPoint.newExactQuery(name, f)
      case d: Double => DoublePoint.newExactQuery(name, d)
      case _ => null
    }
  }
  override def toString: String = s"term query $name = $value"
}

/**
  * And condition query
  * @param query
  */
case class Must(query: MyQuery*) extends MyQuery{

  override def build(): Query = {
    val bool =  new BooleanQuery.Builder()
    for(q <- query){
      bool.add(q.build(), Occur.MUST)
    }
    bool.build()
  }
  override def toString: String = s"bool must "+query.mkString(" AND ")
}

/**
  * OR condition query
  * @param query
  */
case class Should(query: MyQuery*) extends MyQuery{
  override def build(): Query = {
    val bool = new BooleanQuery.Builder()
    for (q <- query) {
      bool.add(q.build(), Occur.SHOULD)
    }
    bool.build()
  }
  override def toString: String = s"bool should "+query.mkString(" OR ")
}

object MyQuery{

  private def dropNull(query: Seq[MyQuery]): Seq[MyQuery] = {
    query.filter( _ != null ).filter(!_.isInstanceOf[MatchAll])
  }

  def term(name: String, value: Any): TermEqual = TermEqual(name, value)

  def must(query: MyQuery*): MyQuery = {
    val q = dropNull(query)
    if(q.length < 1){
      matchAll
    }else if(q.length == 1){
      q.head
    }else{
      Must(q: _*)
    }
  }

  def should(query: MyQuery*): Should = Should(query: _*)

  def matchAll : MatchAll = new MatchAll()

}
