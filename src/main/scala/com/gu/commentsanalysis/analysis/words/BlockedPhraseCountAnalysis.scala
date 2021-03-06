package com.gu.commentsanalysis.analysis.words

import com.gu.commentsanalysis.dao.DataStore
import com.gu.commentsanalysis.{CommentWasBlocked, Cleanup, Util}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object BlockedPhraseCountAnalysis {

  def run()(implicit dataStore: DataStore, sc: SparkContext, cleanup: Cleanup) {
    val broadcastCleanup = sc.broadcast(cleanup)
    val allComments = dataStore.comments
    val blockedComments = allComments.filter(_._2.wasBlocked)

    val allWordcounts = getWordcounts(allComments, broadcastCleanup).filter(_._2._1 > 50)
    val blockedWordcounts = getWordcounts(blockedComments, broadcastCleanup)

    val joined = allWordcounts.join(blockedWordcounts)
      .map{case (word, ((allCount, allExample), (blockedCount, blockedExample)))
      => (word, blockedCount/allCount, blockedCount, allExample, blockedExample)}
    Util.save(joined, "blockedPhraseWordcount")

  }

  def getWordcounts(comments: RDD[(String, CommentWasBlocked)], broadcastCleanup: Broadcast[Cleanup])
                   (implicit dataStore: DataStore): RDD[((String, String), (Double, String))] = {
    comments.map(_._2.comment)
      .flatMap{body => broadcastCleanup.value.cleanupPhrases(body)}
      .reduceByKey(Util.addIntString)
      .map{case (phrase, (intCount, example)) => (phrase, (intCount.toDouble, example))}
  }
}
