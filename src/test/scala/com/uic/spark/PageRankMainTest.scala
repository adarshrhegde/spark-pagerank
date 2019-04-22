package com.uic.spark

import com.typesafe.config.ConfigFactory
import org.scalatest._

class PageRankMainTest extends FlatSpec {

  trait PageRank {
    val pageRank = new PageRankMain
  }

  trait SparkSession extends PageRank {
    val spark = pageRank.getSparkSession
  }

  trait Config {
    val config = ConfigFactory.load("test_application.conf")
  }

  def setup(config: com.typesafe.config.Config) : Unit =  {
    if(!config.getString("spark.hadoopDir").equals(""))
      System.setProperty("hadoop.home.dir", config.getString("spark.hadoopDir"))
  }



  "XML to tuple mapping" should "generate list of tuples from xml" in
    new SparkSession with Config {

      setup(config)

      val df = spark.sqlContext.read.format(config.getString("spark.xmlLibrary"))
        .option("rowTag", config.getString("spark.rowTag"))
        .load(config.getString("spark.xmlFilePath"))

      assert(pageRank.createMappings(spark, df).size == 38)

  }

  "Tuples to Page Rank" should "calculate page rank for nodes" in new SparkSession with Config {

    setup(config)

    val mappings = List(("url_1","url_4"), ("url_2", "url_1"), ("url_3", "url_2"), ("url_3", "url_1"),
      ("url_4", "url_3"), ("url_4", "url_1"))

    val pageRanks = pageRank.computePageRank(spark, mappings, config.getInt("spark.iterations"))

    assert(pageRanks("url_1") == 1.4313779845858583)
    assert(pageRanks("url_2") == 0.4633039012638519)
    assert(pageRanks("url_3") == 0.7294952436130331)
    assert(pageRanks("url_4") == 1.3758228705372555)
    assert(pageRanks.size == 4)

  }

  "XML without row tags" should "return empty list" in new SparkSession with Config {

    setup(config)

    val df = spark.sqlContext.read.format(config.getString("spark.xmlLibrary"))
      .option("rowTag", "nonExistingRowTag")
      .load(config.getString("spark.xmlFilePath"))

    assert(pageRank.createMappings(spark, df).size == 0)

  }

  "Empty mapping list" should "return empty page rank map" in new SparkSession with Config {

    setup(config)

    val pageRanks = pageRank.computePageRank(spark, List(), config.getInt("spark.iterations"))

    assert(pageRanks.size == 0)
  }

  "Two item mapping" should "return page rank 1 for each entity" in new SparkSession with Config {

    setup(config)

    val pageRanks = pageRank.computePageRank(spark, List(("url_1","url_2"),("url_2","url_1")), config.getInt("spark.iterations"))
    assert(pageRanks("url_1") == 1.0)
    assert(pageRanks("url_2") == 1.0)
    assert(pageRanks.size == 2)

  }


}
