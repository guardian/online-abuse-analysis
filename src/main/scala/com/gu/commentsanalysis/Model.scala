package com.gu.commentsanalysis

case class AuthorInfo(author: String, gender: String, mostCommonSection: String,
                      totalArticles: Int, totalArticlesWithAnyComments: Int,
                      totalComments: Int, totalCommentsNotBlocked: Int, totalCommentsBlocked: Int, authorurl: String)

case class ContentInfo(url: String, gender: String, year: String, section: String, tags: List[String])

trait Grouping {
  def getGrouping(info: ContentInfo, word: String): List[GenderGroupingClass]
  val name: String
}
object GenderYearWordGrouping extends Grouping with java.io.Serializable {
  override val name = "YearSectionWord"
  override def getGrouping(info: ContentInfo, word: String): List[GenderGroupingClass] =
    List(GenderYearWordGroupingClass(info.gender, YearSectionWordGroupingClass(info.year, info.section, word)))
}

object GenderYearTagWordGrouping extends Grouping with java.io.Serializable {
  override val name = "YearSectionTagWord"
  override def getGrouping(info: ContentInfo, word: String): List[GenderGroupingClass] =
    info.tags.map(tag => GenderYearTagWordGroupingClass(info.gender, YearSectionTagWordGroupingClass(info.year, info.section, tag, word)))
}

trait GenderGroupingClass{
  val gender: String
  val groupingClass: GroupingClass
  def isFemale = gender.equals("female")
}
case class GenderYearWordGroupingClass(gender: String,  groupingClass: YearSectionWordGroupingClass)
  extends GenderGroupingClass
case class GenderYearTagWordGroupingClass(gender: String, groupingClass: YearSectionTagWordGroupingClass)
  extends GenderGroupingClass

trait GroupingClass extends Ordered[GroupingClass]{
  val year: String
  override def compare(that: GroupingClass): Int = {
    this match{
      case ywgc1: YearSectionWordGroupingClass =>
        that match {
          case ywgc2: YearSectionWordGroupingClass =>
            if(ywgc1.year.equals(ywgc2.year))
              ywgc1.section.compareTo(ywgc2.section)
            else
            ywgc1.year.compareTo(ywgc2.year)
          case _ => throw new Exception("Error incompatible grouping classes")
        }
      case ytwgc1: YearSectionTagWordGroupingClass =>
        that match {
          case ytwgc2: YearSectionTagWordGroupingClass =>
            if(ytwgc1.year.equals(ytwgc2.year))
              if(ytwgc1.section.equals(ytwgc2.section))
                ytwgc1.tag.compareTo(ytwgc2.tag)
              else
                ytwgc1.section.compareTo(ytwgc2.section)
            else
              ytwgc1.year.compareTo(ytwgc2.year)
          case _ => throw new Exception("Error incompatible grouping classes")
        }
      case _ => throw new Exception("Error incompatible grouping classes")
    }
  }
  def string: String
}
case class YearSectionWordGroupingClass(year: String, section: String, word: String) extends GroupingClass {
  override def string: String = year + "," + section + "," + word
}

case class YearSectionTagWordGroupingClass(year: String, section: String, tag:String, word: String) extends GroupingClass {
  override def string: String = year + "," + section + "," + tag + "," + word
}

case class GroupingCount(groupingClass: GroupingClass, femaleRatio: Double, femaleCount: Int) extends Ordered[GroupingCount] {
  override def compare(that: GroupingCount): Int = {
    if(this.groupingClass.compare(that.groupingClass)==0)
      if(this.femaleRatio == that.femaleRatio)
        -this.femaleCount.compareTo(that.femaleCount)
      else
      -this.femaleRatio.compareTo(that.femaleRatio)
    else
      this.groupingClass.compare(that.groupingClass)
  }
}

case class CommentWasBlocked(comment: String, wasBlocked: Boolean)
case class TotalCommentsNotBlockedBlocked(totalComments: Int, notBlocked: Int, blocked: Int)
case class ContentInfoBlockedWordCountExample(contentInfo: ContentInfo, blocked: Boolean, word: String, count: Int, example: String)
case class UrlBlockedWordCountExample(url: String, blocked: Boolean, word: String, count: Int, example: String)