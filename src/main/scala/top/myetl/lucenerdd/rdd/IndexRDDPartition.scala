package top.myetl.lucenerdd.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{DirectoryReader, IndexReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.Directory
import org.apache.spark.Partition
import top.myetl.lucenerdd.store.HdfsDirectory

class IndexRDDPartition(idx: Int, path: String) extends Partition{

  println("LuceneRDDPartition: "+path)

  def config = new Configuration()

  def directory: Directory = HdfsDirectory(new Path(path), config)

  def indexWriter: IndexWriter= new IndexWriter(directory, new IndexWriterConfig().setOpenMode(OpenMode.CREATE_OR_APPEND))

  def indexReader: IndexReader = DirectoryReader.open(directory)

  def indexSearcher: IndexSearcher = new IndexSearcher(indexReader)

  override def index: Int = idx
}