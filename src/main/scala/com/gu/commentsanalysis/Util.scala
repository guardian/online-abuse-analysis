package com.gu.commentsanalysis

import java.util.Calendar
import com.gu.commentsanalysis.dao.{CommentsDB, S3Reader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}

object Util {

  def countByPartition[A](rdd: RDD[A], name: String) = {
    if(Config.log) {
      println(name)
      println(rdd.cache().mapPartitions(iter => Iterator(iter.length)).collect().deep.mkString(", "))
    }
  }

  def save[A](rdd: RDD[A], location:String) = {
    println("loggggiinnnggg: saving")
    val msformat = new java.text.SimpleDateFormat("yyyy-MM-dd.HH:mm.")
    val dayformat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    rdd.coalesce(1).saveAsTextFile("s3n://ophan-temp/comments-analysis-mahana/" + dayformat.format(Calendar.getInstance().getTime)
      + "/" + msformat.format(Calendar.getInstance().getTime)
      + location)
  }



  def getTransformedCommentsCounts(optionComments: Option[TotalCommentsNotBlockedBlocked]): (Int, Int, Int, Int, Int) = {
    val TotalCommentsNotBlockedBlocked(num, notBlocked, blocked) = optionComments.getOrElse(TotalCommentsNotBlockedBlocked(0,0,0))
    (1, if (num > 0)  1 else 0, num, notBlocked, blocked)
  }

  def addIntString(t: (Int, String), p: (Int, String))= (p._1 + t._1, t._2)

  def fiveTupleAdd(t: (Int, Int, Int, Int, Int), p: (Int, Int, Int, Int, Int)) =
   (p._1 + t._1, p._2 + t._2, p._3 + t._3, p._4 + t._4, p._5 + t._5)


  def getCleanup(sQLContext: SQLContext, genders: DataFrame): Cleanup = {
    val stopwords = S3Reader.getStopWords(sQLContext).map{case Row(word: String) => word.toUpperCase}.collect().toSet
    val authorNames = getAuthorNames(genders)
    val commenterNames = CommentsDB.getCommentorNames(sQLContext)
    println("got cleanup")
    Cleanup(authorNames, stopwords, commenterNames)
  }

  def getAuthorNames(genders: DataFrame): Set[String] = {
    genders.flatMap{case Row(_, author: String, _, _, _, _) => author.toUpperCase.split("\\s")}.collect().toSet
  }

}
