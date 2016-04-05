package com.gu.commentsanalysis

import com.typesafe.config.ConfigFactory

object Config {
  val log = true

  val conf = ConfigFactory.load

  val discussionDbUrl = conf.getString("discussionDbUrl")
  val discussionDbUser = conf.getString("discussionDbUser")
  val discussionDbPassword = conf.getString("discussionDbPassword")

  val redshiftUrl = conf.getString("redshiftUrl")
  val redshiftUser = conf.getString("redshiftUser")
  val redshiftPassword = conf.getString("redshiftPassword")

  val awsAccessKeyId = conf.getString("fs.s3n.awsAccessKeyId")
  val awsSecretAccessKey = conf.getString("fs.s3n.awsSecretAccessKey")
}
