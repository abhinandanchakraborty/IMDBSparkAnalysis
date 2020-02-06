package com.imdb.spark.config

import org.apache.spark.sql.SparkSession

object SparkCommonConfig {

  def initSparkSession(master: String, appName: String): SparkSession = {
    val spark = SparkSession.builder.master(master).appName(appName)
      .config("spark.sql.broadcastTimeout", "800s")
      .config("spark.sql.tungsten.enabled", "true")
      //.config("spark.eventLog.enabled", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.rdd.compress", "true")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      .config("spark.yarn.historyServer.allowTracking", "true")
      .config("spark.shuffle.memoryFraction", "1")
      .getOrCreate()
    return spark
  }
  
   def initSparkSession(): SparkSession = {
    val spark = SparkSession.builder
      .config("spark.sql.broadcastTimeout", "800s")
      .config("spark.sql.tungsten.enabled", "true")
      //.config("spark.eventLog.enabled", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.rdd.compress", "true")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      .config("spark.yarn.historyServer.allowTracking", "true")
      .config("spark.shuffle.memoryFraction", "1")
      .getOrCreate()
    return spark
  }

}