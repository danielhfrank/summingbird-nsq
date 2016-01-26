package com.stripe.summingbird.nsq

import com.twitter.summingbird.TimeExtractor
import com.twitter.util.{Await, Promise, Future}

class NSQPushPlan[T: TimeExtractor](sourceRecPairs: Seq[(NSQPushSource[T], Receiver[T])]) {

  val closingTime = new Promise[Boolean]()

  val consumers = sourceRecPairs.map{
    case (source, receiver) => source.buildNSQConsumer(receiver)
  }

  def run(): Future[Unit] = {
    Future.collect(consumers.map{ consumer =>
      // Consumer.start simply opens up connections, background threads do the work
      consumer.start()
      // once outer close has been called, close the consumer
      closingTime.map(_ => consumer.close())
    }).map(_ => Unit)
    // TODO something to do with cleaning up outstanding futures
  }

  def close(): Unit = {
    closingTime.setValue(true)
  }

}
