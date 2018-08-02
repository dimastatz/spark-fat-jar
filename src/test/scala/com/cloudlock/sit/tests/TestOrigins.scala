package com.dimastatz.sit.tests

import org.scalatest._

class TestOrigins extends Matchers with WordSpecLike {

  "Origins logic" must {

    "Get most granular id" in {
      val x = 1
      x should be (1)
    }

    "Test args parsing" in {
      val args = Array("key1=value1", "key2=value2")
      val map = args
        .map(i => i.split("="))
        .groupBy(i => i(0))
        .map(i => (i._1, i._2(0)(1)))

      map.keys.toList.length equals  2
      map("key1") should be ("value1")
    }
  }
}
