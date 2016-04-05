package com.gu.commentsanalysis.dao

import com.gu.commentsanalysis._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object DataStore{
  def apply(sQLContext: SQLContext, sc: SparkContext, cleanup: Cleanup, genders: DataFrame) = new DataStore(sQLContext, sc, cleanup, genders)
}

class DataStore(sQLContext: SQLContext, sc: SparkContext, cleanup: Cleanup, genders: DataFrame) extends java.io.Serializable{

  val authorLookup = Redshift.getAuthorLookup(sQLContext)
  val urlGenderAuthors = Redshift.getUrlGenderAuthors(genders)(sQLContext, this).cache()
  val contentInfo = getContentInfo.cache()
  val comments = getComments.cache()
  val commentSummary = getCommentSummary.cache()
  val counts = getCounts.cache()

  def getComments: RDD[(String, CommentWasBlocked)] = {
    CommentsDB.getComments(sQLContext).map{case Row(body: String, url: String, blocked: Boolean, _) =>
      (url, CommentWasBlocked(Cleanup.removeMarkdownJsoup(body).replace(",",""), blocked))}
  }

  def getContentInfo: RDD[ContentInfo] = {
    val urlGenders = getUrlGenders
    val urlTags = Redshift.getUrlTags(sQLContext)
    val urlYears = Redshift.getUrlYearSection(sQLContext)

    val urlGenderYearTags = urlGenders.join(urlYears).join(urlTags)
    urlGenderYearTags.map{case (url, ((gender, (year, section)),tags)) => ContentInfo(url, gender, year, section, tags)}
  }

  def getUrlGenders : RDD[(String, String)] = {
    urlGenderAuthors.select(urlGenderAuthors("article_url"), urlGenderAuthors("gender"))
      .map {case Row(url: String, gender: String) => (url, List(gender))}
      .reduceByKey { case (genders1, genders2) => List.concat(genders1, genders2).distinct }
      .map { case (url, genderList) => (url, {
        genderList match {
          case head :: Nil => head
          case _ => "mixed"
        }
        genderList.headOption.getOrElse("mixed")
      })
      }
  }

  def getCommentSummary: RDD[(String, TotalCommentsNotBlockedBlocked)] = {
    comments.map { case (url, CommentWasBlocked(_, blocked)) => (url, blocked) }
      .groupByKey()
      .map { case (url, blockedBooleans) =>
        val numComments = blockedBooleans.size
        val notBlocked = blockedBooleans.count(identity(!_))
        val blocked = blockedBooleans.count(identity)
        (url, TotalCommentsNotBlockedBlocked(numComments, notBlocked, blocked))
      }
  }

  def getCounts: RDD[ContentInfoBlockedWordCountExample] = {
    val broadcastContentInfo = sc.broadcast(contentInfo.map(ci => (ci.url, ci)).collectAsMap())

    getUrlWordCountExample.flatMap{
      case UrlBlockedWordCountExample(url, blocked, word, count, example) =>
        broadcastContentInfo.value.get(url) map {
          info => ContentInfoBlockedWordCountExample(info, blocked, word, count, example)
        }
    }
  }

  def getUrlWordCountExample: RDD[UrlBlockedWordCountExample] = {

    val broadcastCleanup = sc.broadcast(cleanup)
    comments.flatMap{case (url, CommentWasBlocked(body, blocked)) =>
          broadcastCleanup.value.cleanupWords(body).map((url, blocked, _))}
      .map{case (url, blocked, (word, (count, example))) => ((url, blocked, word), (count, example))}
      .reduceByKey(Util.addIntString)
      .map{case ((url, blocked, word), (count, example)) => UrlBlockedWordCountExample(url, blocked, word, count, example)}
  }
}


