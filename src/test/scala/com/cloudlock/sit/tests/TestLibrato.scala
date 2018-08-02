package com.dimastatz.sit.tests

import com.dimastatz.sit.adapters._
import org.scalatest._

class TestLibrato extends Matchers with WordSpecLike {
  "Librato logic" must {
    "send metrics" in {
      val email = System.getenv("librato_email")
      val token = System.getenv("librato_token")

      if (email != null) {
        val connection = LibratoAdapter.createConnection(email, token, "sit.umb.dev", "sit.umb", "normalizer-spark")

        val m = ("dima", 2d, "some", "some")
        val sender = LibratoAdapter.sendMetrics(connection) _
        sender(Array(m))
        true should be(true)
      }
    }
  }
}
