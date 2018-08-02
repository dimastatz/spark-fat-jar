package com.dimastatz.sit.utilities

import scala.util._
import org.apache.spark.sql._
import scala.collection.mutable
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast
import com.dimastatz.sit.contracts.Pipeline._
import com.dimastatz.sit.adapters.AwsS3Adapter
import org.joda.time.{DateTime, DateTimeZone}


object Umbrella extends Serializable {

  private val baseDate = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC)
  private val bytesInMB = 1048576

  private val proxy = Set("url-proxy", "url-proxy-https")

  private val blockedApps = Set("blocked", "botnet",
    "domaintagging", "malware", "nxdomain", "phish", "refused", "security")

  private val outputDateFormat = getUmbrellaTimeFormat

  def getUmbrellaTimeFormat: SimpleDateFormat = {
    val format = new SimpleDateFormat("y-MM-dd'T'hh:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("UTC"))
    format
  }

  def loadLogs(session: SparkSession, path: String): DataFrame = {
    session.sqlContext.read.parquet(path).select(
      "timestamp", "handling", "categories", "qname", "origin_id")
  }

  def loadTenantOrgs(session: SparkSession, orgsPath: String, tenantsPath: String): DataFrame = {
    val tenants = session.sqlContext.read.option("sep", ",")
      .option("header", "true").csv(tenantsPath).select("orgId").select("orgId")

    val identities = session.sqlContext.read.option("sep", ",").option("header", "true").csv(orgsPath)
    identities.join(tenants, Seq("orgId")).select("orgId", "originTypeId", "originId", "label")
  }

  def loadParquetLogs(session: SparkSession, logsPath: String,
                      orgsPath: String, tenantsPath: String): Dataset[Row] = {

    val logs = session.sqlContext.read.parquet(logsPath)
      .select("timestamp", "handling", "categories", "qname", "origin_id")
      .groupBy("handling", "categories", "qname", "origin_id")
      .agg(max("timestamp").as("timestamp"), count("qname").as("qname_count"))

    val identities = loadTenantOrgs(session, orgsPath, tenantsPath)

    identities.join(logs, logs("origin_id") === identities("originId")).select("timestamp",
      "handling", "categories", "qname", "orgId", "originTypeId", "originId", "label", "qname_count")
  }

  def loadCsvLogs(session: SparkSession, logsPath: String,
                  orgsPath: String, tenantsPath: String): Dataset[Row] = {

    val logs = session.sqlContext.read
      .format("csv")
      .option("codec", "snappy")
      .option("sep", " ")
      .option("inferSchema", "true")
      .option("header", "false")
      .load(logsPath)

    val logsWithHeaders = logs.select(
      logs("_c0").as("timestamp"),
      logs("_c5").as("handling"),
      logs("_c6").as("origin_id"),
      logs("_c8").as("qname"),
      logs("_c13").as("categories"))
      .groupBy("handling", "categories", "qname", "origin_id")
      .agg(max("timestamp").as("timestamp"), count("qname").as("qname_count"))

    val identities = loadTenantOrgs(session, orgsPath, tenantsPath)
    identities.join(logsWithHeaders, logsWithHeaders("origin_id") === identities("originId")).select("timestamp",
      "handling", "categories", "qname", "orgId", "originTypeId", "originId", "label", "qname_count")
  }

  def loadTenants(session: SparkSession, path: String): DataFrame = {
    session.sqlContext.read.option("sep", ",").option("header", "true").csv(path)
  }

  def broadcastTenants(session: SparkSession, path: String): Broadcast[Seq[(Int, String, String)]] = {
    val map = session.sqlContext
      .read.option("sep", ",").option("header", "true")
      .csv(path)
      .select("orgId", "tenantId", "deviceId")
      .collect()
      .map(i => (i.getString(0).toInt, i.getString(1), i.getString(2)))
      .toSeq


    session.sparkContext.broadcast(map)
  }

  def loadIdentities(session: SparkSession, path: String): DataFrame = {
    import session.sqlContext.implicits._
    val identities = session.sqlContext.read.option("sep", ",").option("header", "true").csv(path)

    Try(loadExcludedOrgs(session, path)) match {
      case Success(x) => identities.except(identities.join(x, $"orgId" === $"org_id").drop("org_id"))
      case _ => identities
    }
  }

  def loadExcludedOrgs(session: SparkSession, path: String): DataFrame = {
    val excludedOrgsPath = path.split('/').dropRight(1).mkString("/") + "/excluded.csv"
    val df = session.sqlContext.read.option("sep", ",").option("header", "true").csv(excludedOrgsPath)

    // invoke to force lazy + validate format
    df.select("org_id").show(1)
    df
  }

  def tryNormalizeParquetRow(row: Row, isProd2: Boolean = false): (Int, Try[NormalizedRow]) = {
    Try(normalizeParquetRow(row, isProd2)) match {
      case Success(x) => (Try(row.getString(4).toInt).getOrElse(0), Success(x))
      case Failure(x) => (Try(row.getString(4).toInt).getOrElse(0), Failure(x))
    }
  }

  def normalizeParquetRow(row: Row, isProd2: Boolean = false): NormalizedRow = {
    // if prod2 put origin_id else build label from type_id and label
    val label = if (isProd2) s"${row.getString(6)}" else
      s"${row.getString(5)}|${row.getString(7)}"

    val qname_count = row.getLong(8).toInt

    NormalizedRow(
      timeStamp = outputDateFormat.format(new Date(row.getTimestamp(0).getTime)),
      label,
      qname = normalizeDomainName(row.getString(3)),
      blocked = parseBlockedValue(row.getString(1), row.getAs[mutable.WrappedArray[Int]](2)),
      in = qname_count,
      out = qname_count,
      sum = qname_count)
  }

  def tryNormalizeCsvRow(row: Row, isProd2: Boolean = false): (Int, Try[NormalizedRow]) = {
    Try(normalizeCsvRow(row, isProd2)) match {
      case Success(x) => (Try(row.getString(4).toInt).getOrElse(0), Success(x))
      case Failure(x) => (Try(row.getString(4).toInt).getOrElse(0), Failure(x))
    }
  }

  def decodeCategories(categories: String): Array[Int] = {
    // Decode categories from hex to array[int]
    val i = BigInt(categories, 16).toString(2)
    i.slice(2, i.length).reverse.zipWithIndex.filter(_._1 == '1').map(_._2).toArray
  }

  def parseTai64nDate(timestamp: String): String = {
    // tai64n timestamp to timestamp
    val seconds = Integer.parseInt(timestamp.slice(9, 17), 16)
    baseDate.plusSeconds(seconds).toString("y-MM-dd'T'hh:mm:ss")
  }

  def normalizeCsvRow(row: Row, isProd2: Boolean = false): NormalizedRow = {
    // if prod2 put origin_id else build label from type_id and label
    val label = if (isProd2) s"${row.getString(6)}" else s"${row.getString(5)}|${row.getString(7)}"
    val categories = decodeCategories(row.getString(2))

    val qname_count = row.getLong(8).toInt

    NormalizedRow(
      timeStamp = parseTai64nDate(row.getString(0)),
      label,
      qname = normalizeDomainName(row.getString(3)),
      blocked = parseBlockedValue(row.getString(1), categories),
      in = qname_count,
      out = qname_count,
      sum = qname_count)
  }


  def parseBlockedValue(handling: String, categories: mutable.WrappedArray[Int]): Int = {
    if (blockedApps.contains(handling) || proxy.contains(handling) && categories.contains(151)) 1 else 0
  }

  def normalizeDomainName(domain: String): String = {
    val normalized = if (domain.endsWith("."))
      domain.take(domain.length - 1).trim else domain.trim

    if (!normalized.contains(",")) normalized
    else normalized.split(',').takeRight(1).headOption.getOrElse("")
  }

  def save(path: String, session: SparkSession,
           tenants: Broadcast[Seq[(Int, String, String)]],
           data: (Int, Iterable[Try[NormalizedRow]])): Seq[Metric] = {

    val success = data._2.filter(_.isSuccess).map(_.get)
    val failures = data._2.filter(_.isFailure).map(_.failed.get)

    val map = tenants.value.groupBy(_._1).mapValues(i => (i.head._2, i.head._3))
    val (tenant, device) = map.getOrElse(data._1, ("unknown", data._1.toString))

    val totalBytes = success.grouped(1000000).map(i => save(
      path, i.map(_.productIterator.mkString(",")).mkString("\n"), tenant, device)).sum * 2

    failures.grouped(1000000).foreach(i => save(
      path, i.toString().mkString("\n"), s"${tenant}_failed", device))

    val totalSuccessSum = success.map(_.in).sum

    List(
      Metric("failed.events.count", failures.toList.length, tenant, device),
      Metric("processed.events.count", totalSuccessSum, tenant, device),
      Metric("processed.agg_events.count", success.toList.length, tenant, device),
      Metric("processed.events.sizeMB", totalBytes / bytesInMB, tenant, device))
  }

  def save(path: String, text: String, tenant: String, device: String): Long = {
    AwsS3Adapter.save(path, text, tenant, device)
    text.length.toLong
  }
}
