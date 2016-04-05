package com.gu.commentsanalysis

import org.jsoup.Jsoup

case class Cleanup(authorNames: Set[String], stopWords: Set[String], commenterNames: Set[String]) {

  // format (word, (count, exampleCommentBody))
  def cleanupWords(input: String): List[(String, (Int, String))] ={
    val split = Cleanup.removeMarkdownPunctuation(input).split("\\s").filter(_.length > 0)
    val removed = split.filter(!authorNames.contains(_)).filter(!stopWords.contains(_)).filter(!commenterNames.contains(_))
    removed.map(word => (word, 1))
      .groupBy(_._1)
      .map{case(word, wcs) => (word, (wcs.map(_._2).sum, "\"" + input + "\""))}
    .toList
  }

  // format ((word1, word2), (count, exampleCommentBody))
  def cleanupPhrases(input: String): Array[((String, String), (Int, String))] ={
    val split = Cleanup.removeMarkdownPunctuation(input).split("\\s").filter(_.length > 0).toList
    val length = split.length
    if(length>1) {

      val firstWords = split.take(length - 1)
      val secondWords = split.tail

      firstWords.zip(secondWords)
        .map((_, 1))
        .groupBy(_._1)
        .map { case (words, wcs) => (words, (wcs.map(_._2).sum, "\"" + input + "\"")) }
        .toArray
    }else{
      Array()
    }
  }
}

object Cleanup {
  def removeMarkdownPunctuation(text: String): String = {
    text.toUpperCase
    .replace("&quot", "QUOTECHARACTER")
      .replace("blockquote", " ")
//      .replaceAll("""<(?!\/?a(?=>|\s.*>))\/?.*?>""", "") // Remove markdown
      .replaceAll("""[\p{Punct}&&[^.]]""", "") // Remove punctuation
      .replace('.', ' ') // Replace full stops with spaces
      .replace('\u2028', ' ')
      .replace('\u201C', ' ')
      .replace('\u2018', ' ')
      .filter(_ >= ' ')  // Remove tabs, new lines etc
      .replaceAll("[^A-Za-z0-9 ]", "")
      .trim
  }

  def removeMarkdownJsoup(text: String): String = {
    Jsoup.parse(text).body().text()
  }
}
