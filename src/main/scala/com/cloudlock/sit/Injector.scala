package com.dimastatz.sit

import scala.util.Try
import com.google.inject._
import org.apache.spark.sql._
import com.dimastatz.sit.Injector._
import com.dimastatz.sit.adapters._
import com.dimastatz.sit.contracts._
import com.dimastatz.sit.utilities._
import com.dimastatz.sit.contracts.Pipeline._
import net.codingwell.scalaguice.ScalaModule
import com.librato.metrics.client.PostMeasuresResult
import com.typesafe.scalalogging.slf4j.LazyLogging

object Injector {
  type MetricSender = Seq[(String, Double, String, String)] => PostMeasuresResult
}

class Injector(args: Array[String]) extends AbstractModule with ScalaModule with LazyLogging {
  override def configure(): Unit = {
    logger.info("configuring injector")
  }

  @Provides
  @Singleton
  def getConfig: Configuration = {
    logger.info(s"creating config $args")
    val config = Configuration.parse(args)
    config
  }

  @Provides
  @Singleton
  def getSparkSession: SparkSession = {

    val config = getConfig
    logger.info(s"creating session ${config.spark_master.isEmpty}")

    if (config.spark_master.isEmpty)
      SparkSession
        .builder()
        .appName("normalizer")
        .getOrCreate()
    else
      SparkSession
        .builder()
        .appName("normalizer")
        .master(config.spark_master.get)
        .getOrCreate()
  }

  @Provides
  @Singleton
  def getMetricsSender: MetricSender = {
    val config = getConfig.metricsConfig

    val connection = LibratoAdapter.Connection(
      LibratoAdapter.Credentials(config.email, config.token),
      config.cluster, serviceName = config.serviceName)

    LibratoAdapter.sendMetrics(connection)
  }


  @Provides
  @Singleton
  def getPipeline: Pipeline = {
    val config = getConfig
    val sender = getMetricsSender
    val session = getSparkSession

    val isProd2Mode = AwsS3Adapter.isProd2Mode(config.tenantsFilePath)
    val tenants = Umbrella.broadcastTenants(session, config.tenantsFilePath)

    val loadLogsFunction  = if(config.inputLogsFormat == Configuration.parquetFormat)
      Umbrella.loadParquetLogs _ else Umbrella.loadCsvLogs _

    val normalizeLogsFunction = if(config.inputLogsFormat == Configuration.parquetFormat)
      Umbrella.tryNormalizeParquetRow _ else Umbrella.tryNormalizeCsvRow _

    new {} with Pipeline {
      def load(): Dataset[Row] = loadLogsFunction(session,
        config.inputLogsPath, config.identitiesFilePath, config.tenantsFilePath)

      def normalize(row: Row): (Int, Try[NormalizedRow]) =
        normalizeLogsFunction(row, isProd2Mode)

      def save(orgData: (Int, Iterable[Try[NormalizedRow]])): Seq[Metric] =
        Umbrella.save(config.normalizedLogsPath, session, tenants, orgData)

      override def sendMetrics(metrics: Seq[Metric]): Unit =
        sender(metrics.map(i => (i.name, i.value, i.tenant, i.device)))
    }
  }
}
