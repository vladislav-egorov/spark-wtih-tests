package com.epam

import org.apache.spark.sql.SparkSession

object Main {
  val wordCounter = new WordCounter;

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Cinderella word count")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");

    val text = spark.read.textFile("src/main/resources/text.txt")
    wordCounter.countWords(text, spark)
      .show(20);
  }

}
