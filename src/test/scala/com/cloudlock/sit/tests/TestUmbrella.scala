package com.dimastatz.sit.tests

import org.scalatest._
import java.sql.Timestamp
import org.apache.spark.sql._
import com.dimastatz.sit.utilities.Umbrella


class TestUmbrella extends Matchers with WordSpecLike {
  private val session = SparkSession.builder()
    .appName("test").master("local").getOrCreate()

  "Umbrella functionality" must {
    "load parquet logs" in {
      val logsPath = getClass.getResource("/umbrella.parquet").getPath
      val identitiesPath = getClass.getResource("/umbrella_id.txt").getPath
      val tenantsPath = getClass.getResource("/umbrella_org.txt").getPath

      val df = Umbrella.loadParquetLogs(session, logsPath, identitiesPath, tenantsPath)

      df.show(false)
      df.count() should be (3)
      df.schema.length should be (9)

      val row = df.take(1).head
      row.get(0).isInstanceOf[Timestamp] should be (true)
      //row.get(2).isInstanceOf[mutable.WrappedArray[Int]] should be (true)

      val normalized = Umbrella.normalizeParquetRow(row)
      normalized != null should be (true)
    }

    "load csv logs" in {
      //val logsPath = getClass.getResource("/umbrella_eu.snappy").getPath
      //val identitiesPath = getClass.getResource("/umbrella_id.txt").getPath
      //val df = Umbrella.loadCsvLogs(session, logsPath, identitiesPath)
      //df.show(3)

      //df.count() > 0 should be (true)
    }

    "do parquet normalization" in {

      val qnameCount = 1L
      val categories: Seq[Int] = Array(1, 2, 3)
      val categories151: Seq[Int] = Array(1, 151, 3)

      val row1 = Row(new Timestamp(1514766116648L), "normal",
        categories, "p15-ckcoderouter.fe.apple-dns.net.", "8834613", "b", "1", "a", qnameCount)
      val row2 = Row(new Timestamp(1514766116648L), "botnet",
        categories, "p15-ckcoderouter.fe.apple-dns.net.", "8834613", "b", "1", "a", qnameCount)
      val row3 = Row(new Timestamp(1514766116648L), "botnet",
        categories, "db._dns-sd._udp.media.global.loc,jbcp.co.uk.", "8834613", "b", "1", "a", qnameCount)
      val row4 = Row(new Timestamp(1514766116648L), "botnet",
        categories, "p15,ckcoderouter.fe,apple-dns.net.", "8834613", "b", "1", "a", qnameCount)
      val row5 = Row(new Timestamp(1514766116648L), "botnet",
        categories, "p15,ckcoderouter.fe,apple-dns.net.", "8834613", "b", "1", "a", qnameCount)
      val row6 = Row(new Timestamp(1514766116648L), "normal",
        categories151, "p15,ckcoderouter.fe,apple-dns.net.", "8834613", "b", "1", "a", qnameCount)
      val row7 = Row(new Timestamp(1514766116648L), "url-proxy",
        categories151, "p15,ckcoderouter.fe,apple-dns.net.", "8834613", "b", "1", "a", qnameCount)

      def expected_values(row: Row, date: String = "2018-01-01T12:21:56", label: String = "b|a",
                          qname: String = "p15-ckcoderouter.fe.apple-dns.net", block: Int = 0, isProd2: Boolean = false) {
        val normalized = Umbrella.normalizeParquetRow(row, isProd2)

        normalized.timeStamp should be(date)
        normalized.label should be(label)
        normalized.qname should be(qname)
        normalized.blocked should be(block)
      }
      //test all relevant fields normalized.
      expected_values(row1)

      //test block field get 1 due to "botnet" value.
      expected_values(row2, "2018-01-01T12:21:56", "b|a", "p15-ckcoderouter.fe.apple-dns.net", 1)

      //test qname get last domain after comma and without trailing dot (only 1 comma exist).
      expected_values(row3, "2018-01-01T12:21:56", "b|a", "jbcp.co.uk", 1)

      //test qname get last domain after comma and without trailing dot (2 comma's exist).
      expected_values(row4, "2018-01-01T12:21:56", "b|a", "apple-dns.net", 1)

      //test label get only 5'th field value when IsProd2 = true
      expected_values(row5, "2018-01-01T12:21:56", "1", "apple-dns.net", 1, true)

      //test categories contain 151 but handling value not in proxy - block return 0
      expected_values(row6, "2018-01-01T12:21:56", "1", "apple-dns.net", 0, true)

      //test categories contain 151 and handling value in proxy - block return 1
      expected_values(row7, "2018-01-01T12:21:56", "1", "apple-dns.net", 1, true)
    }
  }
}
