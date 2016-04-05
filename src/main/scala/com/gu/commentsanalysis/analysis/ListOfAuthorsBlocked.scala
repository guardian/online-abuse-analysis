package com.gu.commentsanalysis.analysis

import com.gu.commentsanalysis.{CommentWasBlocked, AuthorInfo, Util}
import com.gu.commentsanalysis.dao.DataStore
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

object ListOfAuthorsBlocked {

  val authorList = List("Denis MacShane","Chris Elliott","Seumas Milne","Clive Stafford Smith","John Domokos",
                        "Ian Birrell","Owen Jones","Ken Livingstone","Martin Rowson","Ally Fogg",
                        "Nick Cohen","Michael Wolff",
                        "Jessica Valenti", "Nesrine Malik","Julie Bindel","Bidisha","Barbara Ellen",
                        "Jill Filipovic","Leah Green","Caterina Monzani","Tanya Gold","Suzanne Moore",
                        "Steven W Thrasher","Joseph Harker","Nick Cohen","Tariq Ali")


  def run() (implicit datastore: DataStore, sc: SparkContext)= {
    val articles = sc.broadcast(getUrlGenderAuthorRatio)
    datastore.comments.filter(_._2.wasBlocked).cache()
      .filter{case(url, _) => articles.value.keySet.contains(url)}.cache()
      .map{case (url, CommentWasBlocked(body, _)) =>
        val (gender, author, ratio) = articles.value.getOrElse(url, ("", "", 0d))
        ((author, gender, ratio), (url, body))}.cache()
      .groupByKey()
      .collect()
      .map{case ((author, gender, ratio), iter) => ((author, gender, ratio), sc.parallelize(iter.toList, 1).cache())}
      .foreach{case ((authorUrl, gender, ratio), rdd) =>
        val proportion = Math.min(1d, 500d/rdd.count)
        val authorTag = authorUrl.replace("http://www.theguardian.com/profile/", "")
        Util.save(rdd.sample(false, proportion), "comments/" + datastore.authorLookup.getOrElse(authorUrl, "") + "-" + gender + "-" + ratio.toString + "-" + authorTag)}
  }

  def getUrlGenderAuthorRatio(implicit datastore: DataStore, sc: SparkContext): Map[String, (String, String, Double)] = {
    val authorBlockrate = AuthorAnalysis.getAuthorInfo()
      .collect().toList
      .filter(ai => authorList.contains(ai.author))
      .map(ai => (ai.authorurl, ai.totalCommentsBlocked.toDouble/ai.totalComments, ai)).sortBy(_._2)

    printList(authorBlockrate)

    val allAuthors = authorBlockrate.map{case (author, ratio, _) => (author, ratio)}.toMap

    datastore.urlGenderAuthors.map{case Row(url: String, gender: String, authorUrl: String) => (url, (gender, authorUrl))}
            .filter{case (_, (_, authorUrl)) => allAuthors.contains(authorUrl)}
//            .filter{case (url, (_,_)) => thisYearsContent.value.contains(url)}
            .map{case (url, (gender, authorUrl)) =>
              val ratio = allAuthors.getOrElse(authorUrl, 0d)
              (url, (gender, authorUrl, ratio))
            }
      .collect().toMap
  }

  def printList(list: List[(String, Double, AuthorInfo)]): Unit = {
    list.zipWithIndex.foreach { case ((author, num, ai), index) =>
      print(index)
      print(" ")
      print(ai)
      print(", ")
      println(num) }
  }
}
