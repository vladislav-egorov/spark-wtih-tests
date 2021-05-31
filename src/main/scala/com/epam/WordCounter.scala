package com.epam

import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

class WordCounter {
  def countWords(dataset: Dataset[String], spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    dataset
      .flatMap(line => line.split(" "))
      .filter("value != ''")
      .map(word => (word, 1))
      .withColumn("word", functions.regexp_replace(col("_1"), "[$&+,:;=?@#|'<>.^*()%!-]", ""))
      .withColumnRenamed("_2", "count")
      .groupBy("word")
      .sum("count")
      .orderBy(desc("sum(count)"))
  }
}