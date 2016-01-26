package com.stripe.summingbird.nsq

case class NSQClientConfig(topic: String,
                           channel: String,
                           lookupAddrs: Seq[(String, Int)]) extends Serializable
