package com.gu.commentsanalysis.dao

import org.apache.spark.sql.{DataFrame, SQLContext}

object S3Reader {
  def getGenders(implicit sQLContext: SQLContext): DataFrame = {
    sQLContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("s3n://ophan-temp/comments-analysis-mahana/gender.csv")
  }

  def getStopWords(implicit sQLContext: SQLContext): DataFrame = {
    sQLContext.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("s3n://ophan-temp/comments-analysis-mahana/stopwords.csv")
  }
}
