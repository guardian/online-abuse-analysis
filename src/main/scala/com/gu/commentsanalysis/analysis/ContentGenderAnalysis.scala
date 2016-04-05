package com.gu.commentsanalysis.analysis

import com.gu.commentsanalysis.dao.DataStore
import com.gu.commentsanalysis.{Util, Main, ContentInfo}
import org.apache.spark.rdd.RDD

object ContentGenderAnalysis {

  def run()(implicit datastore: DataStore) {
    val articlesByGenderYearSection = datastore.contentInfo.map{ci => ((ci.gender, ci.year, ci.section), 1)}
      .reduceByKey(_+_)
      .map{case ((gender, year, section), count) => (gender, year, section, count)}
    Util.save(articlesByGenderYearSection, "ArticlesByGenderYearSection")
    val articlesByGenderYearSectionTag = datastore.contentInfo.flatMap{ci => ci.tags.map(tag => ((ci.gender, ci.year, ci.section, tag),1))}
      .reduceByKey(_+_)
      .map{case ((gender, year, section, tag), count) => (gender, year, section, tag, count)}
    Util.save(articlesByGenderYearSectionTag, "ArticlesByGenderYearSectionTag")
  }
}
