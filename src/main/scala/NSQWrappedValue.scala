import com.trendrr.nsq.NSQMessage
import com.twitter.util.Future

import scala.collection.{GenTraversableOnce, TraversableOnce}

case class NSQWrappedValue[T](messageObject: NSQMessage, underlying: Future[TraversableOnce[T]]) {

  // ----- praying these are the only methods we actually use

  def map[B](f: T => B): NSQWrappedValue[B] = new NSQWrappedValue[B](messageObject, underlying.map(_.map(f)))

  /**
   *
   * @param f One to many
   * @tparam B
   * @return
   */
  def flatMapTrav[B](f: T => GenTraversableOnce[B]): NSQWrappedValue[B] = new NSQWrappedValue[B](messageObject, underlying.map(_.flatMap(f)))

  /**
   *
   * @param f One to Future[One]
   * @tparam B
   * @return
   */
  def flatMapFut[B](f: T => Future[B]): NSQWrappedValue[B] = new NSQWrappedValue[B](messageObject, underlying.flatMap{ underlyingActual =>
    Future.collect(underlyingActual.toList.map(f))
  })
}

object NSQWrappedValue {

  def factory[T](decodeFn : (Array[Byte]) => TraversableOnce[T])(message: NSQMessage): NSQWrappedValue[T] =
    new NSQWrappedValue[T](message, Future.value(decodeFn(message.getMessage)))

}
