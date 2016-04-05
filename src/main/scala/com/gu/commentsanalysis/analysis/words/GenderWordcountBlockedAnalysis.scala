package com.gu.commentsanalysis.analysis.words

import com.gu.commentsanalysis.dao.DataStore
import com.gu.commentsanalysis._

/**
 * Created by mahanaclutha on 30/11/15.
 */
object GenderWordcountBlockedAnalysis {
  def run()(implicit datastore: DataStore) {
    analyseByGrouping(GenderYearTagWordGrouping)
    analyseByGrouping(GenderYearWordGrouping)
  }

  def analyseByGrouping(grouping: Grouping)(implicit datastore: DataStore) = {
    val genderGroupingCounts = datastore.counts
      .filter(_.blocked)
      .flatMap{case ContentInfoBlockedWordCountExample(info,blocked, word, count, example) =>
        grouping.getGrouping(info, word).map(g => (g, (count, example)))}.cache()

    val femaleGroupingCounts = genderGroupingCounts
      .filter(_._1.isFemale)
      .map{case (ggc, (count, example)) => (ggc.groupingClass, (count, example))}
      .reduceByKey(Util.addIntString)
      .filter(_._2._1 > 20)

    val overallGroupingCounts = genderGroupingCounts
      .map{case (ggc, (count, example)) => (ggc.groupingClass, (count, example))}
      .reduceByKey(Util.addIntString)

    val femaleIndex = femaleGroupingCounts.join(overallGroupingCounts)
      .map{case (gc, ((female_count, femaleExample), (allCount, allExample))) =>
        (gc.string, female_count.toDouble / allCount, female_count, femaleExample, allExample)}
    //        .map{case (gc, (female_count, allCount)) => GroupingCount(gc, female_count.toDouble / allCount, female_count)}
    //      .sortBy(identity)
    //      .map{case GroupingCount(gc, femaleRatio, femaleCount) => (gc.string, femaleRatio, femaleCount)}

    Util.save(femaleIndex, grouping.name + "blocked")
  }
}
