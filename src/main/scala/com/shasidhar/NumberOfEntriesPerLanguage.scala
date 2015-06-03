package com.shasidhar

import org.apache.spark.SparkContext

/**
 * Created by hadoop on 3/6/15.
 */
object NumberOfEntriesPerLanguage {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0),"No of entries per language")

    val inputRdd = sc.textFile(args(1))

    val languageRdd = inputRdd.map(eachRow=>{
      (eachRow.split(" ")(1),1)
    }).reduceByKey(_+_)

    println("Number of entries per Language \n"+languageRdd.collect().mkString("\n"))
  }
}
