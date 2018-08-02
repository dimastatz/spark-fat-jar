package com.dimastatz.sit.utilities


object Origins {
  private val origins = Array(36, 34, 9, 11, 7, 5, 19, 24, 3, 15, 28, 30, 21, 13, 32, 1, 17, 22, 26)
  private val originsGranularityMap = origins.zipWithIndex.groupBy(_._1).map(i => (i._1, i._2(0)_2))

  def resolveOriginId(record: Seq[(String, Int)]): String = {
    resolveOriginId(originsGranularityMap, record)
  }

  def resolveOriginId(granularityMap: Map[Int, Int], record: Seq[(String, Int)]): String = {
    record
      .map(i => (i, granularityMap(i._2)))
      .sortWith((x, y) => x._2 < y._2)
      .map(i => i._1._1)
      .head
  }
}
