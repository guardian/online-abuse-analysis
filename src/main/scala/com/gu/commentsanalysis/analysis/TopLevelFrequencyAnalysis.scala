package com.gu.commentsanalysis.analysis

import java.sql.Date

import com.gu.commentsanalysis.Util
import com.gu.commentsanalysis.dao.{CommentsDB, Redshift}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class UserArticleCommentSummary(user: Int, url: String, visibleComments: Long, invisibleComments: Long)
case class ContentSummary(url: String, section: String, publicationDate: Date, commentable: Boolean, premoderated: Boolean)

case class Summary(user: Int, url: String, visibleComments: Long, invisibleComments: Long,
                   publicationDate: Date, commentable: Boolean, premoderated: Boolean)

case class UserSummary(user:Int, visibleComments: Long, invisibleComments: Long)

object TopLevelFrequencyAnalysis {
  def run(implicit sQLContext: SQLContext, sc: SparkContext) = {
    val contentCommentsSummary = Redshift.getContentCommentsSummary
    val userArticleCommentSummary = CommentsDB.getUserArticleCommentSummary

    val collectedContent = contentCommentsSummary.collect()

    val contentUrlMap = collectedContent.map(cs => (cs.url, cs)).toMap
    val broadcastContentUrlMap = sc.broadcast(contentUrlMap)

    val contentSectionMap = collectedContent.filter(_.commentable).map(cs => (cs.section, cs))
      .groupBy(_._1)
      .map{case(section, css) => (section, {
        val commentable = css.map(_._2.url).distinct.length
        val premoderated = css.filter(_._2.premoderated).map(_._2.url).distinct.length
        (commentable, premoderated)}
        )}
    val broadcastConentSectionMap = sc.broadcast(contentSectionMap)

    val summary = userArticleCommentSummary. flatMap { uacs =>
      broadcastContentUrlMap.value.get(uacs.url) map {
        cs => (cs.section, Summary(uacs.user, uacs.url, uacs.visibleComments,  uacs.invisibleComments,
         cs.publicationDate, cs.commentable, cs.premoderated))
      }
    }

    val sectionGroup = summary.groupByKey()

    val result = sectionGroup.map{case (section, summaries) =>
      val uniques = summaries.map(_.user).toList.distinct.size
      val commentablePremoderated = broadcastConentSectionMap.value.getOrElse(section, (0,0))
      val commentableArticles = commentablePremoderated._1
      val commentablePremoderatedArticles = commentablePremoderated._2
      val totalVisibleComments = summaries.map(_.visibleComments).sum
      val invisibleComments = summaries.map(_.invisibleComments).sum

      val userTotals = summaries.map(s => (s.user, (s.visibleComments, s.invisibleComments)))
        .groupBy(_._1)
        .map{case (user, list) => (user, list.map(_._2))}
        .map{case (user, list) => (user, list.foldLeft((0l,0l))((a,b) => (a._1 + b._1, a._2 + b._2)))}

      val totalCommentsOnce = userTotals.filter{case (user, (vis, invis)) => vis + invis == 1}.toList

      val usersCommentOnceVisible = totalCommentsOnce.filter{case (user, (vis, invis)) => vis == 1}.map(_._1).distinct.size
      val usersCommentOnceInvisible = totalCommentsOnce.filter{case (user, (vis, invis)) => invis == 1}.map(_._1).distinct.size

      val usersCommentOnceTotal = totalCommentsOnce.map(_._1).distinct.size

      (section.replace(",","").replace(";",""), uniques, commentableArticles, commentablePremoderatedArticles,
        totalVisibleComments, invisibleComments, usersCommentOnceTotal, usersCommentOnceVisible, usersCommentOnceInvisible)
    }

    Util.save(result, "topLevelFigures")

  }
}
