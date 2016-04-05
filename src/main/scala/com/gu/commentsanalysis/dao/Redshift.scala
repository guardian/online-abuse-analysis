package com.gu.commentsanalysis.dao

import java.sql.Date

import com.gu.commentsanalysis.Config
import com.gu.commentsanalysis.analysis.ContentSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

object Redshift {

//  object Database {
//
//    def getQueryResults(sql: String)(implicit sQLContext: SQLContext): DataFrame = {
//      sQLContext.read
//        .format("com.databricks.spark.redshift")
//        .option("url", Config.redshiftUrl + "?user=" + Config.redshiftUser + "&password=" + Config.redshiftPassword)
//        .option("query", sql)
//        .option("tempdir", "s3n://comments-analysis/temp")
//        .load()
//    }
//
//    def getArticleYearSection(implicit sQLContext: SQLContext): DataFrame = {
//      getQueryResults("select url, year, section from temp_mm.article_year_section where url is not null and year is not null and section is not null")
//    }
//
//    def getArticleAuthors(implicit sQLContext: SQLContext): DataFrame = {
//      getQueryResults("select author_url, article_url, author from temp_mm.authorurl_articleurl_author where author_url is not null and article_url is not null and author is not null")
//    }
//
//    def getArticleTags(implicit sQLContext: SQLContext): DataFrame = {
//      getQueryResults("select keyword, url from temp_mm.tag_articleurl where keyword is not null and url is not null")
//    }
//
//    def getAuthorNameUrl(implicit sQLContext: SQLContext): DataFrame = {
//      getQueryResults("select name, url from temp_mm.authorname_authorurl where name is not null and url is not null")
//    }
//
//    def getContentCommentsSummary(implicit sQLContext: SQLContext): DataFrame = {
//      getQueryResults("select web_url, section_name, dt, commentable, premoderated from temp_mm.content_comments_summary where section_name is not null" +
//        " and dt is not null and web_url is not null")
//    }
//  }


  object Database {

    def getQueryResults(sql: String)(implicit sQLContext: SQLContext): DataFrame = {
      sQLContext.sql(sql)
//        .format("com.databricks.spark.redshift")
//        .option("url", Config.redshiftUrl + "?user=" + Config.redshiftUser + "&password=" + Config.redshiftPassword)
//        .option("query", sql)
//        .option("tempdir", "s3n://comments-analysis/temp")
//        .load()
    }

    def getArticleYearSection(implicit sQLContext: SQLContext): DataFrame = {
      getQueryResults("select url, year, section from temp_mm.article_year_section where url is not null and year is not null and section is not null")
    }

    def getArticleAuthors(implicit sQLContext: SQLContext): DataFrame = {
      getQueryResults("select author_url, article_url, author from temp_mm.authorurl_articleurl_author where author_url is not null and article_url is not null and author is not null")
    }

    def getArticleTags(implicit sQLContext: SQLContext): DataFrame = {
      getQueryResults("select keyword, article_url as url from temp_mm.tag_articleurl where keyword is not null and article_url is not null")
    }

    def getAuthorNameUrl(implicit sQLContext: SQLContext): DataFrame = {
      getQueryResults("select name, url from temp_mm.authorname_authorurl where name is not null and url is not null")
    }

    def getContentCommentsSummary(implicit sQLContext: SQLContext): DataFrame = {
      getQueryResults("select web_url, section_name, dt, commentable, premoderated from temp_mm.content_comments_summary where section_name is not null" +
        " and dt is not null and web_url is not null")
    }
  }

  def getContentCommentsSummary(implicit sQLContext: SQLContext): RDD[ContentSummary] = {
    Database.getContentCommentsSummary
    .map{case Row(url: String, section: String, dt: Date, commentable: Boolean, premoderated: Boolean) =>
      ContentSummary(url, section, dt, commentable, premoderated)}
  }

  def getUrlYearSection(implicit sQLContext: SQLContext): RDD[(String, (String, String))] = {
    Database.getArticleYearSection
      .map{case Row(url: String, year: Double, section: String) => (url, (getYear(year.toInt), section))}
  }

  def getUrlTags(implicit sQLContext: SQLContext) : RDD[(String, List[String])] = {
    Database.getArticleTags
      .map{case Row(keyword: String, url: String) => (url, keyword.replaceAll(",", " "))}
      .groupByKey()
      .map{case(url, keywordIter) => (url, keywordIter.toList)}
  }

  def getAuthorLookup(implicit sQLContext: SQLContext): Map[String, String] = {
    Database.getAuthorNameUrl
      .map{case Row(name: String, url: String) => (url, name)}
      .collect().toMap
  }

  def getUrlGenderAuthors(genders: DataFrame)(implicit sQLContext: SQLContext, dataStore: DataStore)  : DataFrame = {
    val contentAuthors = Database.getArticleAuthors
    val joined = contentAuthors.join(genders, "author")
    joined.select(joined("article_url"), joined("gender"), joined("author_url"))
  }

  def getYear(year: Int): String = {
    if (year.equals(12))
      "2012"
    else if (year.equals(14))
      "2014"
    else
      year.toString
  }

}
