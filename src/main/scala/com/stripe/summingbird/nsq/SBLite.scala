package com.stripe.summingbird.nsq

import com.trendrr.nsq.{NSQMessage, NSQMessageCallback}
import com.twitter.algebird.Aggregator
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.Future
import org.slf4j.{LoggerFactory, Logger}


abstract class SBLite[T, K, V](agg: Aggregator[T, V, _],
                               decoder: Array[Byte] => Option[T],
                               keyFxn: T => TraversableOnce[K],
                               store: MergeableStore[K,V]) extends NSQMessageCallback{

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def error(e: Exception): Unit = System.exit(1)

  override def message(nsqMessage: NSQMessage): Unit = {
    decoder(nsqMessage.getMessage)
      .map { msgData =>
      val v = agg.prepare(msgData)
      val kvMap = keyFxn(msgData)
        .map(_ -> v)
        .toMap
      Future.collect(store.multiMerge(kvMap).values.toList)
    }
      .getOrElse {
      logger.info("Giving up on " + nsqMessage.getId.toString)
      Future.value(Nil)
    }
      .onSuccess(_ => nsqMessage.finished())
      .onFailure(_ => nsqMessage.requeue())
  }
}
