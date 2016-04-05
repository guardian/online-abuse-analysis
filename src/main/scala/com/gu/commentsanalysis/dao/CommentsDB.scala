package com.gu.commentsanalysis.dao

import com.gu.commentsanalysis.Config
import com.gu.commentsanalysis.analysis.UserArticleCommentSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

object CommentsDB {

  def getComments(implicit sQLContext: SQLContext) : DataFrame = {
    getQueryResultsBigTable(sQLContext)
//    getQueryResultsSmallTable(sQLContext)
  }

  def getQueryResultsBigTable(implicit sQLContext: SQLContext) : DataFrame = {
    getQueryResultsUsualSetup(" select body, url, blocked, n from public.mm_comment_body_url_blocked", "100")
  }

  def getQueryResultsSmallTable(implicit sQLContext: SQLContext) : DataFrame = {
    getQueryResultsUsualSetup(" select body, url, blocked, n from public.mm_comment_body_url_blocked_small", "50")
  }

  def getQueryResultsUsualSetup(sql: String, numPartitions: String)(implicit sQLContext: SQLContext) : DataFrame = {
    getQueryResults(sql, "n", "1001", "1", numPartitions)
  }

  def getQueryResults(sql: String, partitionColumn: String, upperbound: String, lowerbound: String, numPartitions: String)(implicit sQLContext: SQLContext) : DataFrame = {
    sQLContext.read.format("jdbc")
    .option("url", Config.discussionDbUrl +  "?user=" + Config.discussionDbUser + "&password=" + Config.discussionDbPassword)
    .option("dbtable", "( "+ sql + " ) a")
    .option("upperBound", upperbound)
    .option("numPartitions", numPartitions)
    .option("driver", "org.postgresql.Driver")
    .option("partitionColumn", partitionColumn)
    .option("lowerBound", lowerbound)
    .load()
  }

  def getQueryResultsSmall(sql: String)(implicit sQLContext: SQLContext) : DataFrame = {
    sQLContext.read.format("jdbc")
      .option("url", Config.discussionDbUrl +  "?user=" + Config.discussionDbUser + "&password=" + Config.discussionDbPassword)
      .option("dbtable", "( "+ sql + " ) a")
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def getCommentorNames(implicit sQLContext: SQLContext): Set[String] ={
    getQueryResultsSmall("select distinct username from profiles_profile").map{case Row(name: String) => name}.collect().toSet
  }

  def getUserArticleCommentSummary(implicit sQLContext: SQLContext): RDD[UserArticleCommentSummary] = {
    getQueryResultsUsualSetup("select * from public.mm_user_article_comments", "50")
    .map{case Row(user: Int, url: String, visibleComments: Long, invisibleComments: Long, _) =>
      UserArticleCommentSummary(user, url, visibleComments, invisibleComments)}
  }
}
