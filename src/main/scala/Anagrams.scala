import scala.collection.mutable


object Anagrams {

  /** A word is simply a `String`. */
  type Word = String

  /** A sentence is a `List` of words. */
  type Sentence = List[Word]

  /** `Occurrences` is a `List` of pairs of characters and positive integers saying
   *  how often the character appears.
   *  This list is sorted alphabetically w.r.t. to the character in each pair.
   *  All characters in the occurrence list are lowercase.
   *
   *  Any list of pairs of lowercase characters and their frequency which is not sorted
   *  is **not** an occurrence list.
   *
   *  Note: If the frequency of some character is zero, then that character should not be
   *  in the list.
   */
  type Occurrences = List[(Char, Int)]

  /** The dictionary is simply a sequence of words.
   *  It is predefined and obtained as a sequence using the utility method `loadDictionary`.
   */
  val dictionary: List[Word] = forcomp.loadDictionary

  /** Converts the word into its character occurrence list.
   *
   *  Note: the uppercase and lowercase version of the character are treated as the
   *  same character, and are represented as a lowercase character in the occurrence list.
   *
   *  Note: you must use `groupBy` to implement this method!
   */
  def wordOccurrences(w: Word): Occurrences = {
    w.toLowerCase.toList.groupBy(c => c).map(a => (a._1, a._2.length)).toList.sortBy(a => a._1)
  }

  /** Converts a sentence into its character occurrence list. */
  def sentenceOccurrences(s: Sentence): Occurrences = {
    s.groupBy(c => c)
      .map(a => (wordOccurrences(a._1), a._2.length))
      .map(a => a._1.map(c => (c._1, c._2 * a._2))).flatten
      .groupBy(c => c._1).map(a => (a._1, a._2.map(_._2).sum)).toList.sortBy(a => a._1)
  }

  /** The `dictionaryByOccurrences` is a `Map` from different occurrences to a sequence of all
   *  the words that have that occurrence count.
   *  This map serves as an easy way to obtain all the anagrams of a word given its occurrence list.
   *
   *  For example, the word "eat" has the following character occurrence list:
   *
   *     `List(('a', 1), ('e', 1), ('t', 1))`
   *
   *  Incidentally, so do the words "ate" and "tea".
   *
   *  This means that the `dictionaryByOccurrences` map will contain an entry:
   *
   *    List(('a', 1), ('e', 1), ('t', 1)) -> Seq("ate", "eat", "tea")
   *
   */
  lazy val dictionaryByOccurrences: Map[Occurrences, List[Word]] = {
    //List[Word]
    dictionary.map(a => (wordOccurrences(a), a)).groupBy(a => a._1).map(a => (a._1, a._2.map(_._2)))
  }

  /** Returns all the anagrams of a given word. */
  def wordAnagrams(word: Word): List[Word] = dictionaryByOccurrences(wordOccurrences(word))

  /** Returns the list of all subsets of the occurrence list.
   *  This includes the occurrence itself, i.e. `List(('k', 1), ('o', 1))`
   *  is a subset of `List(('k', 1), ('o', 1))`.
   *  It also include the empty subset `List()`.
   *
   *  Example: the subsets of the occurrence list `List(('a', 2), ('b', 2))` are:
   *
   *    List(
   *      List(),
   *      List(('a', 1)),
   *      List(('a', 2)),
   *      List(('b', 1)),
   *      List(('a', 1), ('b', 1)),
   *      List(('a', 2), ('b', 1)),
   *      List(('b', 2)),
   *      List(('a', 1), ('b', 2)),
   *      List(('a', 2), ('b', 2))
   *    )
   *
   *  Note that the order of the occurrence list subsets does not matter -- the subsets
   *  in the example above could have been displayed in some other order.
   */

  // list (char int) -> list(list(char int))
  def combinations(occurrences: Occurrences): List[Occurrences] = buildCartesian(null, occurrences).map(_.sortBy(_._1))

  def buildCartesian(a : List[Occurrences], occurrences: Occurrences) : List[Occurrences] = {
    if (a == null)
      if (occurrences.size > 1) {
        buildCartesian(expandByNum(occurrences.head), occurrences.tail)}
      else List(occurrences)
    else {
      val head = expandByNum(occurrences.head) // List[List[(char, Int)], List...]
      println(head)
      var ret = a
      for (o : Occurrences <- a) {
        for (b <- head) {
          for (pair <- b) ret :+= (o :+ pair)
        }
      }
      for (v <- head) ret +:= v
      if (occurrences.size > 1) {
        buildCartesian(ret, occurrences.tail)
      }
      else ret :+ List()
    }
  }

  def expandByNum(tuples: (Char, Int)) : List[Occurrences] = {
    var ret = List[List[(Char, Int)]]()
    for (i <- 1 to tuples._2) {
      ret :+= List((tuples._1, i))
    }
    ret
  }

  /** Subtracts occurrence list `y` from occurrence list `x`.
   *
   *  The precondition is that the occurrence list `y` is a subset of
   *  the occurrence list `x` -- any character appearing in `y` must
   *  appear in `x`, and its frequency in `y` must be smaller or equal
   *  than its frequency in `x`.
   *
   *  Note: the resulting value is an occurrence - meaning it is sorted
   *  and has no zero-entries.
   */
  def subtract(x: Occurrences, y: Occurrences): Occurrences = {
    //y is subset
    val map = mutable.HashMap[Char, Int](x :_*)
    for (_y <- y) {
      val in =  map(_y._1) - _y._2
      map.put(_y._1, in)
    }
    map.toList.filter(a => a._2 > 0).sortBy(_._1)
  }

  /** Returns a list of all anagram sentences of the given sentence.
   *
   *  An anagram of a sentence is formed by taking the occurrences of all the characters of
   *  all the words in the sentence, and producing all possible combinations of words with those characters,
   *  such that the words have to be from the dictionary.
   *
   *  The number of words in the sentence and its anagrams does not have to correspond.
   *  For example, the sentence `List("I", "love", "you")` is an anagram of the sentence `List("You", "olive")`.
   *
   *  Also, two sentences with the same words but in a different order are considered two different anagrams.
   *  For example, sentences `List("You", "olive")` and `List("olive", "you")` are different anagrams of
   *  `List("I", "love", "you")`.
   *
   *  Here is a full example of a sentence `List("Yes", "man")` and its anagrams for our dictionary:
   *
   *    List(
   *      List(en, as, my),
   *      List(en, my, as),
   *      List(man, yes),
   *      List(men, say),
   *      List(as, en, my),
   *      List(as, my, en),
   *      List(sane, my),
   *      List(Sean, my),
   *      List(my, en, as),
   *      List(my, as, en),
   *      List(my, sane),
   *      List(my, Sean),
   *      List(say, men),
   *      List(yes, man)
   *    )
   *
   *  The different sentences do not have to be output in the order shown above - any order is fine as long as
   *  all the anagrams are there. Every returned word has to exist in the dictionary.
   *
   *  Note: in case that the words of the sentence are in the dictionary, then the sentence is the anagram of itself,
   *  so it has to be returned in this list.
   *
   *  Note: There is only one anagram of an empty sentence.
   */


  def sentenceAnagrams(sentence: Sentence): List[Sentence] =
    sentenceAnagramsHelper(sentenceOccurrences(sentence))

  def sentenceAnagramsHelper(occurrences: Occurrences): List[Sentence] =  occurrences match {
    case Nil => List(Nil)
    case occurrences: Occurrences =>
      val combs = combinations(occurrences)
      //Set(elem) return a boolean value true if elem exists and false if doesn't
      for {i <- combs if dictionaryByOccurrences.keySet(i)
           j <- dictionaryByOccurrences(i)
           s <- sentenceAnagramsHelper(subtract(occurrences, i))
      } yield j :: s
  }
  def main(args: Array[String]): Unit = {
  }
}
