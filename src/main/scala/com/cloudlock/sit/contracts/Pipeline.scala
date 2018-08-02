package com.dimastatz.sit.contracts

import com.dimastatz.sit.contracts.Pipeline._
import org.apache.spark.sql._

import scala.util._

object Pipeline {

  case class Metric(name: String, value: Double, tenant: String = "all", device: String = "all")

  case class NormalizedRow(timeStamp: String, label: String, in: Int = 1, out: Int = 1,
                           sum: Int = 1, qname: String, port: Int = 53, blocked: Int)

}

trait Pipeline extends Serializable {
  def load(): Dataset[Row]

  def sendMetrics(metrics: Seq[Metric]): Unit

  def normalize(row: Row): (Int, Try[NormalizedRow])

  def save(orgData: (Int, Iterable[Try[NormalizedRow]])): Seq[Metric]

  def run(session: SparkSession): Unit = {
    val start = System.nanoTime()

    val metrics  = load()
      .rdd
      .map(normalize)
      .groupByKey()
      .flatMap(save)
      .collect()

    sendMetrics(createTimeMetric(start) +: metrics)
  }

  def createTimeMetric(start: Long): Metric = {
    Metric("processed.e2e.sec", (System.nanoTime() - start) / 1000000000)
  }
}
