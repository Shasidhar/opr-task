package com.shasidhar

import org.apache.spark.SparkContext

/**
 * Created by hadoop on 3/6/15.
 */
object Top100Languages {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0),"Top 100 languages")

    val inputRdd = sc.textFile(args(1))

    val languageRdd = inputRdd.map(eachRow=>{
      (eachRow.split(" ")(1),1)
    }).reduceByKey(_+_).sortBy(-_._2).take(100)

    println("Top 100 Languages \n"+languageRdd.mkString("\n"))
  }
}
