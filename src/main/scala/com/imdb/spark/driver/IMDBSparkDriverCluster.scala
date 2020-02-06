package com.imdb.spark.driver

import com.imdb.spark.config.SparkCommonConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ AnalysisException, Row }
import org.apache.spark.sql.types.{ IntegerType, LongType, DoubleType }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import java.net.ConnectException

object IMDBSparkDriverCluster extends App {

  val logger = Logger.getLogger(getClass.getName)
  //Spark initialization using passing master and appname for local master will be local or local[<no executor>] else type the name of RM like "yarn"
  val spark = SparkCommonConfig.initSparkSession()

  try {

    val arguments = args
    if (arguments.length == 0) {
      logger.info("Please provide the base path")
      spark.close()
      System.exit(1)
    }
    //set the path for input files
    val basePath = arguments(0)
    val titleAkasPath = basePath + "//" + "akas.tsv"
    val titleBasicsPath = basePath + "//" + "title_basis.tsv"
    val titleRatingsPath = basePath + "//" + "title_ratings.tsv"
    val titlePrincipalPath = basePath + "//" + "title_principals.tsv"
    val nameBasisPath = basePath + "//" + "name_basics.tsv"

    //load the Dataframe for title Akas
    val titleAkasDF = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(titleAkasPath).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //load the Dataframe for title Basics
    val titleBasicsDF = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(titleBasicsPath).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //load the Dataframe for title Principal
    val titlePrincipalDF = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(titlePrincipalPath).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //load the Dataframe for name Basis
    val nameBasisDF = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(nameBasisPath).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //load the Dataframe for rating and do a type cast for numVotes and averageRating from String to Long and Double
    val titleRatingsDF = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(titleRatingsPath)
      .withColumn("numVotes", col("numVotes").cast(LongType)).withColumn("averageRating", col("averageRating").cast(DoubleType)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    /*
   * Logic to build the Top 10 movies based on the derived logic
   */

    //Join movie title with rating data frame using "tconst" and filter based on the "titleType" move only
    val joinTitlewithRating = titleBasicsDF.join(titleRatingsDF, Seq("tconst")).where(col("titleType") === "movie")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //calculate average number of votes based on titleType movies
    val sumNumVotes = joinTitlewithRating.select(avg(col("numVotes"))).first().get(0)

    //Filter the data frame where numVotes at least 50 and create a derived column "rankCal" based on (numVotes/averageNumberOfVotes)*averageRating logic
    val derivedRatingDF = joinTitlewithRating.where(col("numVotes").cast(LongType) >= 50)
      .withColumn("rankCal", lit(((col("numVotes") / sumNumVotes) * col("averageRating"))).cast(DoubleType))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //Abstruct window Operation to find the rank based on the derived column
    val windowSpec = Window.orderBy(desc("rankCal"))

    //Create a column "movieRank" based on derived column "rankCal" and fine the top 10 movies.
    val topMoviesDf = derivedRatingDF.withColumn("movieRank", dense_rank() over windowSpec).where(col("movieRank") <= 10)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    topMoviesDf.coalesce(1).write.option("header", "true").csv(basePath + "//topMoviesDump")

    /*
   * top 10 movies and the person is most often credited
   */

    //Join with principal data frame with top 10 movies data frame to get the person id "nconst"
    val joinwithPrincipalDF = topMoviesDf.join(titlePrincipalDF.select("tconst", "nconst"), Seq("tconst")).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //Join with name.basics.tsv based on the "nconst" to get the person is most often credited
    val mostCreditedPersonDF = joinwithPrincipalDF.join(nameBasisDF.select("nconst", "primaryName"), Seq("nconst")).persist(StorageLevel.MEMORY_AND_DISK_SER)

    mostCreditedPersonDF.orderBy(desc("movieRank")).coalesce(1).write.option("header", "true").csv(basePath + "//mostCreditedPerson")
    /*
   * List of different titles of the 10 movies.
   */

    //Join with title.akas based on the "nconst" to get the list of titles
    val diffTitleMoviesDF = topMoviesDf.join(titleAkasDF.select("titleId", "title", "region", "language").withColumnRenamed("titleId", "tconst"), Seq("tconst"))
      .orderBy(asc("movieRank")).persist(StorageLevel.MEMORY_AND_DISK_SER)

    diffTitleMoviesDF.orderBy(desc("movieRank")).coalesce(1).write.option("header", "true").csv(basePath + "//diffTitleMovies")

  } catch {

    case sslException: InterruptedException => {
      logger.error("Interrupted Exception")
    }
    case nseException: NoSuchElementException => {
      logger.error("No Such element found: " + nseException.printStackTrace())
    }
    case anaException: AnalysisException => {
      logger.error("SQL Analysis Exception: " + anaException.printStackTrace())
    }
    case connException: ConnectException => {
      logger.error("Connection Exception: " + connException.printStackTrace())
    }
    case ex: Exception => {
      logger.error("Exception: " + ex.printStackTrace())
    }
  } finally {
    spark.stop()
  }

}