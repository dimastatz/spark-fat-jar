package com.dimastatz.sit.utilities


object Configuration {
  val parquetFormat: String = "parquet"

  def parse(args: Array[String]): Configuration = {
    val input_log_location = args.filter(
      _.startsWith("input_logs_location")).head.substring(20)

    val otherArgs = args.filter(i => !i.startsWith("input_logs_location"))

    val map = otherArgs
      .filter(_.contains("="))
      .map(_.split("="))
      .groupBy(i => i(0))
      .map(i => (i._1, i._2(0)(1)))

    val metrics = MetricsConfig(
      map("librato_email"),
      map("librato_token"),
      map("cluster_name"),
      map.getOrElse("librato_service_name", "normalizer-spark")
    )

    Configuration(
      input_log_location,
      map.getOrElse("input_logs_format", parquetFormat),
      map("identities_file_location"),
      map("org_tenant_file_location"),
      map("output_logs_location"),
      map.get("spark_master"),
      metrics)
  }
}

case class MetricsConfig(email: String, token: String, cluster: String, serviceName: String)

case class Configuration(inputLogsPath: String,
                         inputLogsFormat: String,
                         identitiesFilePath: String,
                         tenantsFilePath: String,
                         normalizedLogsPath: String,
                         spark_master: Option[String],
                         metricsConfig: MetricsConfig)


