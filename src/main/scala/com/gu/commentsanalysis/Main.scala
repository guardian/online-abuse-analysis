package com.gu.commentsanalysis

import com.gu.commentsanalysis.analysis._
import com.gu.commentsanalysis.analysis.words.{GenderWordcountBlockedAnalysis, GenderWordCountAnalysis, BlockedPhraseCountAnalysis, BlockedWordcountAnalysis}
import com.gu.commentsanalysis.dao.{S3Reader, DataStore}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.postgresql.Driver

object Main {

  def main(args: Array[String]) {

  println("doing stuff")

    implicit val (sQLContext, sc) = setup

    // genders is in format (num, author, firstname, middle, lastname, gender)
    val genders = S3Reader.getGenders(sQLContext).cache()

//    Util.save(genders.rdd, "genders")

    implicit val cleanup = Util.getCleanup(sQLContext, genders)
    implicit val dataStore = DataStore(sQLContext, sc, cleanup, genders)

//    TopLevelFrequencyAnalysis.run
//    BlockedRetriever.run()
//    ListOfAuthorsBlocked.run()
    BlockedAnalysis.run()
    AuthorAnalysis.run()
//    GenderWordCountAnalysis.run()
//    GenderWordcountBlockedAnalysis.run()
//    BlockedWordcountAnalysis.run()
//    BlockedPhraseCountAnalysis.run()
//    ContentGenderAnalysis.run()

  }

  def setup: (SQLContext, SparkContext) = {
    val classes = Seq(
      getClass, // To get the jar with our own code.
      classOf[Driver] // To get the connector.
    )
    val jars = classes.map(_.getProtectionDomain.getCodeSource.getLocation.getPath)
    val conf = new SparkConf().setJars(jars).setAppName("Comments Analysis").set("spark.executor.cores", "1")
    val sc = new SparkContext(conf)

//    sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", Config.awsAccessKeyId)
//    sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", Config.awsSecretAccessKey)
//
//    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Config.awsAccessKeyId)
//    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Config.awsSecretAccessKey)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    (sqlContext, sc)
  }
}