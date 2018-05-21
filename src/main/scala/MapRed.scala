import scala.collection.parallel.immutable.ParVector
import scala.collection.parallel.ParMap
import scala.collection.GenSeq
import scala.collection.GenMap
import scala.io.Source

object MapRed {
  private val stopWords = Source.fromResource("stop_words.txt").getLines.toSeq

  def isStopWord(word: String): Boolean = stopWords.contains(word)

  def words(lines: GenSeq[String]): GenSeq[String] =
    lines.flatMap(_.split(' ')).map(_.toLowerCase)
      .map(_.replaceAll("""[^a-z]""", ""))
      .filter(w => !isStopWord(w) && w != "")

  def mapper(seq: GenSeq[String]): GenSeq[(String, Int)] =
    seq.map(_ -> 1)

  def reducer(seq: GenSeq[(String, Int)]): GenMap[String, Int] =
    seq.groupBy(_._1).mapValues(_.map(_._2).sum)

  def main(args: Array[String]): Unit = {
    val fileIterator = Source.fromResource("SHAKESPEARE.txt").getLines
    val lines = fileIterator.toVector.par //seq
    val result = reducer(mapper(words(lines)))
    println(result.seq.toList.sortBy(-_._2).take(100))
  }
}
