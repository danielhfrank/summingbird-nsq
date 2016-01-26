package com.stripe.summingbird.nsq

import com.trendrr.nsq.NSQMessage
import com.twitter.summingbird.TimeExtractor
import com.twitter.util.Future

import scala.collection.{GenTraversableOnce, TraversableOnce}

case class NSQWrappedValue[T](messageObject: Option[NSQMessage], timestamp: Option[Long], underlying: Future[TraversableOnce[T]]) {
  // messageObject/timestamp is only optional so that we can prime the Stream with a dummy message. :fu: Stream

  // ----- praying these are the only methods we actually use

  def map[B](f: T => B): NSQWrappedValue[B] = new NSQWrappedValue[B](messageObject, timestamp, underlying.map(_.map(f)))

  def foreach(f: T => Unit): NSQWrappedValue[T] = new NSQWrappedValue[T](messageObject, timestamp, underlying.foreach(_.foreach(f)))

  /**
   *
   * @param f One to many
   * @tparam B
   * @return
   */
  def flatMapTrav[B](f: T => GenTraversableOnce[B]): NSQWrappedValue[B] = new NSQWrappedValue[B](messageObject, timestamp, underlying.map(_.flatMap(f)))

  /**
   *
   * @param f One to Future[One]
   * @tparam B
   * @return
   */
  def flatMapFut[B](f: T => Future[B]): NSQWrappedValue[B] = new NSQWrappedValue[B](messageObject, timestamp, underlying.flatMap{ underlyingActual =>
    Future.collect(underlyingActual.toList.map(f))
  })
}

object NSQWrappedValue {

  def factory[T: TimeExtractor](decodeFn : (Array[Byte]) => TraversableOnce[T])(message: NSQMessage): NSQWrappedValue[T] = {
    val messageValue = decodeFn(message.getMessage).toList // we expect this to be zero or one valid messages
    val te = implicitly[TimeExtractor[T]]
    val timestamp = messageValue.headOption.map(te(_))
    new NSQWrappedValue[T](Some(message), timestamp, Future.value(messageValue))
  }
}
