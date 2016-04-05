package com.gu.commentsanalysis.analysis

import com.gu.commentsanalysis.dao.DataStore
import com.gu.commentsanalysis.{CommentWasBlocked, AuthorInfo, Util}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

object BlockedRetriever {


  def run() (implicit datastore: DataStore, sc: SparkContext)= {
    val articles = sc.broadcast(getUrlGenderAuthorRatio)
    datastore.comments.filter(_._2.wasBlocked).cache()
      .filter{case(url,  _) => articles.value.keySet.contains(url)}.cache()
      .map{case (url, CommentWasBlocked(body, _)) =>
        val (gender, author, ratio) = articles.value.getOrElse(url, ("", "", 0d))
        ((author, gender, ratio), (url, body))}.cache()
      .groupByKey()
      .collect()
      .map{case ((author, gender, ratio), iter) => ((author, gender, ratio), sc.parallelize(iter.toList, 1).cache())}
      .foreach{case ((authorUrl, gender, ratio), rdd) =>
        val proportion = Math.min(1d, 1000d/rdd.count)
        val authorTag = authorUrl.replace("http://www.theguardian.com/profile/", "")
        Util.save(rdd.sample(false, proportion), "comments/" + datastore.authorLookup.getOrElse(authorUrl, "") + "-" + gender + "-" + ratio.toString + "-" + authorTag)}
  }

  def getUrlGenderAuthorRatio(implicit datastore: DataStore, sc: SparkContext): Map[String, (String, String, Double)] = {
    val menAndWomen = Set("male", "female")
    val authorBlockrate = AuthorAnalysis.getAuthorInfo()
      .filter(ai => "Comment is free".equals(ai.mostCommonSection)).collect().toList
      .filter(ai => ai.totalArticles > 100 && ai.totalComments > 0)
      .filter(ai => menAndWomen.contains(ai.gender))
      .map(ai => (ai.authorurl, ai.totalCommentsBlocked.toDouble/ai.totalComments, ai)).sortBy(_._2)

    val topTen = authorBlockrate.reverse.take(10)
    println("top 10")
    printList(topTen)
    val bottomTen = authorBlockrate.take(10)
    println("bottom 10")
    printList(bottomTen)

    val allAuthors = (topTen ++ bottomTen).map{case (author, ratio, _) => (author, ratio)}.toMap

    datastore.urlGenderAuthors.map{case Row(url: String, gender: String, authorUrl: String) => (url, (gender, authorUrl))}
            .filter{case (_, (_, authorUrl)) => allAuthors.contains(authorUrl)}
//            .filter{case (url, (_,_)) => thisYearsContent.value.contains(url)}
            .map{case (url, (gender, authorUrl)) =>
              val ratio = allAuthors.getOrElse(authorUrl, 0d)
              (url, (gender, authorUrl, ratio))
            }
      .collect().toMap
  }

//  def getUrlForTags(): RDD[String] = {
//
//  }

  def printList(list: List[(String, Double, AuthorInfo)]): Unit = {
    list.zipWithIndex.foreach { case ((author, num, ai), index) =>
      print(index)
      print(" ")
      print(ai)
      print(", ")
      println(num) }
  }
}
