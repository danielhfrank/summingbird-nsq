package com.stripe.summingbird.nsq

import com.twitter.algebird.Semigroup
import com.twitter.storehaus.{ConcurrentHashMapStore, ReadableStore}
import com.twitter.storehaus.algebra.{Mergeable, MergeableStore}
import com.twitter.summingbird.online.MergeableStoreFactory
import com.twitter.summingbird.{TimeExtractor, Source, Platform}
import com.twitter.summingbird.batch.{Batcher, BatchID, Timestamp}
import com.twitter.util.{Await, Future, FuturePool}

import com.twitter.util.Try

import scala.language.existentials

object Example {

  def storeFor[V](b: Batcher) = new NSQStore[String, V] {
    def batcher = b
    def mergeable(sg: Semigroup[V]): Mergeable[(String, BatchID), V] =
      MergeableStore.fromStoreNoMulti(new ConcurrentHashMapStore[(String, BatchID), V])(sg)
  }

  def printSink(sumResult: (Timestamp, (String, (Option[Int], Int)))): Future[Unit] = {
    val (ts, (k, (prevV, incrV))) = sumResult
    Future.value(println(s"timestamp: $ts, k: $k, prevV: $prevV, incrV: $incrV"))
  }

  // Generic job:
  def job[P <: Platform[P]](src: P#Source[String], store: P#Store[String, Int], sink: P#Sink[(String, (Option[Int], Int))]) =
    Source[P, String](src).map{ s => (s, 1) }
      .sumByKey(store)
      .write(sink)

  def main (args: Array[String]) {
    val clientConfig = NSQClientConfig("test", "summingbird-nsq", Seq(("localhost", 4161)))

    implicit val dummyTimeExtractor = TimeExtractor[String](_ => 0L)
    val batcher = Batcher.ofDays(1)

    val source = new NSQPushSource[String](clientConfig, bytes => Try(new String(bytes, "UTF-8")))
    val store = storeFor[Int](batcher)

    val j = job[NSQ](source, store, printSink _)
    val planned = (new NSQ(FuturePool.unboundedPool)).plan(j)

    // This should never complete
    Await.result(planned.run())
  }
}
