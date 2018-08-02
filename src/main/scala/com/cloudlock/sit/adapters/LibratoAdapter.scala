package com.dimastatz.sit.adapters

import java.util.concurrent._

import com.librato.metrics.client._


object LibratoAdapter {

  case class Credentials(email: String, token: String)

  case class Connection(credentials: Credentials, cluster: String, appId: String = "sit.umb", serviceName: String)


  def createClient(email: String, token: String, appID: String, timeoutSec: Int = 5): LibratoClient = {
    val duration = new Duration(timeoutSec, TimeUnit.SECONDS)

    LibratoClient.builder(email, token)
      .setReadTimeout(duration)
      .setConnectTimeout(duration)
      .setAgentIdentifier(appID)
      .build()
  }

  def createConnection(email: String, token: String, cluster: String, appID: String, serviceName: String): Connection = {
    val credentials = LibratoAdapter.Credentials(email, token)
    LibratoAdapter.Connection(credentials, cluster, appID, serviceName)
  }

  def sendMetrics(connection: Connection)(metrics: Seq[(String, Double, String, String)]): PostMeasuresResult = {
    val client = LibratoAdapter.createClient(
      connection.credentials.email,
      connection.credentials.token,
      connection.appId)

    val measures = new Measures()
    metrics.foreach(x => measures.add(createLibratoMetric(connection, x)))
    client.postMeasures(measures)
  }

  def createLibratoMetric(conn: Connection, metric: (String, Double, String, String)): TaggedMeasure = {
    val tags = Array(
      new Tag("source", "sit"),
      new Tag("tenant_id", metric._3),
      new Tag("device_id", metric._4),
      new Tag("service", conn.serviceName))

    val name = s"${conn.appId}.${metric._1}"
    new TaggedMeasure(name, metric._2, new Tag("cluster", conn.cluster), tags: _*)
  }

}

