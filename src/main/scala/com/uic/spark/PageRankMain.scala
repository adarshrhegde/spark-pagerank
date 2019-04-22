package com.uic.spark

import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class PageRankMain {

  import PageRankMain._


  def createMappings(spark : SparkSession, df : org.apache.spark.sql.DataFrame) :
  List[Tuple2[String, String]] = {

    val accumulator = spark.sparkContext.collectionAccumulator[Tuple2[String, String]]("Mapping")

    logger.info("Count " + df.count())

    df.foreach(article => {
      val authors = article.getAs[mutable.WrappedArray[String]]("author")

      val publication = article.getAs[String]("journal")

      if(null != authors) {

        val authorAndPublication = publication :: authors.toList

        authorAndPublication.combinations(2).toList.foreach(combo => {
          accumulator.add(combo(0), combo(1))
          accumulator.add(combo(1), combo(0))
        })
      }
    })

    accumulator.value.asScala.toList
  }

  def getSparkSession : SparkSession = {

    SparkSession.builder.master("local[*]").appName("Spark Page Rank Prof").getOrCreate()
  }

  def computePageRank(spark : SparkSession, mappings : List[Tuple2[String, String]],
                      iterations : Int) : Map[String, Double] = {

    //val iterations = 10

    var pageRanks : Map[String, Double] = Map()

    val lines = spark.sparkContext.parallelize(mappings)
    val links = lines.distinct().groupByKey().cache()


    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iterations) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => {
      pageRanks += (tup._1 -> tup._2)
      //println(tup._1 + " has rank: " + tup._2 + ".")
    })

    pageRanks
  }

  def endSession(sparkSession: SparkSession) = sparkSession.stop()
}

object PageRankMain {

  val logger = LoggerFactory.getLogger(classOf[PageRankMain])


  def main(args: Array[String]): Unit = {

    logger.info("=========START============")
    val config = ConfigFactory.load()

    // Provide path to winutils.exe for windows environment
    if(!config.getString("spark.hadoopDir").equals(""))
      System.setProperty("hadoop.home.dir", config.getString("spark.hadoopDir"))

    logger.info("Configs loaded  " + config)

    val obj = new PageRankMain

    // Get a spark session object
    val sparkSession = obj.getSparkSession

    /**
      *  Create a sql.Dataframe object from the input file
      *  We use a tag (e.g. "article") to split the xml
      *
      */
    val df = sparkSession.sqlContext.read.format(config.getString("spark.xmlLibrary"))
      .option("rowTag", config.getString("spark.rowTag"))
      .load(args(0))

    // Accumulator is used to collect values from the distributed processes
    val mappings : List[Tuple2[String, String]] = obj.createMappings(sparkSession, df)

    logger.info("Completed creating mappings of size" + mappings.size)


    logger.info("Starting computation of page ranks")

    // Compute the page rank

    val result = obj.computePageRank(sparkSession, mappings, args(2).toInt)
    for ((elem, value) <- result) {

      logger.info(elem + " = " + value)
    }

    sparkSession.sparkContext.parallelize(result.toSeq).saveAsTextFile("output.txt")


    logger.info("Completed computation of page ranks")

    // Stop the spark session
    obj.endSession(sparkSession)

    logger.info("=========STOP============")
  }

}
