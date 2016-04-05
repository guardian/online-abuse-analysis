package com.gu.commentsanalysis.analysis

import com.gu.commentsanalysis.dao.DataStore
import com.gu.commentsanalysis.{Util, Main, ContentInfo}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Output file for each run is of the form:
 *
 * ((details), (totalArticles, totalArticlesWithAnyComments, totalComments, totalCommentsNotBlocked, totalCommentsBlocked))
 *
 * where in the first case
 *
 *   details = gender, year, section
 *
 * and in the second case
 *
 *   details = gender, year, section, tag
 *
 */
object BlockedAnalysis {


  def run() (implicit datastore: DataStore) = {

    val contentTuple = datastore.contentInfo.map(ci => (ci.url, ci))
    val joined = contentTuple.leftOuterJoin(datastore.commentSummary)

    val byArticle = joined.map{case (url, (ci, optionComments)) =>
      ((ci.gender, ci.year, ci.section), Util.getTransformedCommentsCounts(optionComments))}
      .reduceByKey(Util.fiveTupleAdd)

    Util.save(byArticle, "commentsBlockedBySection")

    val byTag = joined.flatMap{case (url, (ci, optionComments)) =>
      ci.tags.map(tag => ((ci.gender, ci.year, ci.section, tag), Util.getTransformedCommentsCounts(optionComments)))}
      .reduceByKey(Util.fiveTupleAdd)

    Util.save(byTag, "commentsBlockedByTag")
  }

}

