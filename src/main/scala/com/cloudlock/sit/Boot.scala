package com.dimastatz.sit

import scala.util._
import com.google.inject
import com.google.inject._
import org.apache.spark.sql._
import com.typesafe.scalalogging.slf4j._
import com.dimastatz.sit.contracts.Pipeline
import com.dimastatz.sit.Injector.MetricSender
import com.librato.metrics.client.PostMeasuresResult
import net.codingwell.scalaguice.InjectorExtensions._

object Boot extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info(s"starting normalizer ${args.deep.mkString("\n")}")
    val injector = Guice.createInjector(new Injector(args))

    Try(initialize(injector)) match {
      case Success(x) => logger.info("normalization executed")
      case Failure(x) =>
        logger.error(s"failed to run normalizer $x")
        Try(reportFailure(injector))
        throw x
    }

    logger.info("finished to run flow")
  }

  private def initialize(injector: inject.Injector): Unit = {
    logger.info("get injected objects")
    val session = injector.instance[SparkSession]
    val pipeline = injector.instance[Pipeline]

    pipeline.run(session)
    logger.info("stopping spark session")
    session.stop()
  }

  private def reportFailure(injector: inject.Injector): PostMeasuresResult = {
    val metric = ("failures.count", 1d, "all", "all")
    val sendMetric = injector.instance[MetricSender]
    sendMetric(Array(metric))
  }
}
