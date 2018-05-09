import scala.collection.parallel.immutable.ParVector
import scala.collection.parallel.ParMap
import scala.io.Source

object MapRed {
  private val stopWords = Source.fromResource("stop_words.txt").getLines.toSeq
  def isStopWord(word: String): Boolean = stopWords.contains(word)

  def words(lines: ParVector[String]): ParVector[String] =
    lines.flatMap(_.split(' ')).map(_.toLowerCase)
      .map(_.replaceAll("""[^a-z]""", ""))
      .filter(w => !isStopWord(w) && w != "")

  def map(seq: ParVector[String]): ParVector[(String, Int)] =
    seq.map(_ -> 1)

  def reduce(seq: ParVector[(String, Int)]): ParMap[String, Int] =
    seq.groupBy(_._1).mapValues(_.map(_._2).reduce(_+_))

  def main(args: Array[String]): Unit = {
    val fileIterator = Source.fromResource("SHAKESPEARE.txt").getLines
    val lines = fileIterator.toVector.par
    val result = reduce(map(words(lines)))
    println(result.seq.toList.sortBy(-_._2).take(100))
  }
}
