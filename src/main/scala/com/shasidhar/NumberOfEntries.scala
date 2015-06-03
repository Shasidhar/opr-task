package com.shasidhar

import org.apache.spark.SparkContext

/**
 * Created by hadoop on 3/6/15.
 */
object NumberOfEntries {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0),"No of entries")

    val inputRdd = sc.textFile(args(1))

    println("number of entires : "+inputRdd.count())
  }
}
