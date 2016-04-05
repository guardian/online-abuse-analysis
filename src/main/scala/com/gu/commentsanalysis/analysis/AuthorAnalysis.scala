package com.gu.commentsanalysis.analysis

import com.gu.commentsanalysis.dao.DataStore
import com.gu.commentsanalysis.{AuthorInfo, Main, ContentInfo, Util}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}


object AuthorAnalysis {

  def run()(implicit datastore: DataStore, sc: SparkContext) = {
    val result: RDD[AuthorInfo] = getAuthorInfo()
    Util.save(result, "authorAnalysis")
  }

  def getAuthorInfo()(implicit datastore: DataStore, sc: SparkContext): RDD[AuthorInfo] = {
    val broadcastLookup = sc.broadcast(datastore.authorLookup)
    datastore.urlGenderAuthors.map { case Row(url: String, gender: String, author: String) => (url, (gender, author)) }
      .join(datastore.contentInfo.map(ci => (ci.url, ci)))
      .leftOuterJoin(datastore.commentSummary)
      .map { case (url, (((gender, author), ci), optionComments)) => ((author, gender), (url, optionComments, ci.section)) }
      .groupByKey()
      .map { case ((authorUrl, gender), iter) =>
        val (totalArticles, totalArticlesWithAnyComments, totalComments, totalCommentsNotBlocked, totalCommentsBlocked)
        = iter.map { case (_, optionComments, _) => Util.getTransformedCommentsCounts(optionComments) }
          .fold(0, 0, 0, 0, 0)(Util.fiveTupleAdd)

        val (mostCommonSection, _) = iter.map { case (_, _, section) => (section, 1) }
          .groupBy(_._1)
          .map { case (sec, secIter) => (sec, secIter.map(_._2).sum) }
          .toList
          .sortBy(-_._2)
          .head

        AuthorInfo(broadcastLookup.value.getOrElse(authorUrl, ""), gender, mostCommonSection, totalArticles,
          totalArticlesWithAnyComments, totalComments, totalCommentsNotBlocked, totalCommentsBlocked, authorUrl)
      }
  }
}
