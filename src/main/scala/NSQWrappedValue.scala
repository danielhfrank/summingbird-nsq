import com.trendrr.nsq.NSQMessage

import scala.collection.{GenTraversableOnce, TraversableOnce}

case class NSQWrappedValue[T](messageObject: NSQMessage, underlying: TraversableOnce[T]) extends TraversableOnce[T]{
  override def copyToArray[B >: T](xs: Array[B], start: Int, len: Int): Unit = underlying.copyToArray(xs, start, len)

  override def seq: TraversableOnce[T] = underlying.seq

  override def hasDefiniteSize: Boolean = underlying.hasDefiniteSize

  override def forall(p: (T) => Boolean): Boolean = underlying.forall(p)

  override def toTraversable: Traversable[T] = underlying.toTraversable

  override def isEmpty: Boolean = underlying.isEmpty

  override def find(p: (T) => Boolean): Option[T] = underlying.find(p)

  override def foreach[U](f: (T) => U): Unit = underlying.foreach(f)

  override def exists(p: (T) => Boolean): Boolean = underlying.exists(p)

  override def toStream: Stream[T] = underlying.toStream

  override def toIterator: Iterator[T] = underlying.toIterator

  override def isTraversableAgain: Boolean = underlying.isTraversableAgain

  // ----- praying these are the only methods we actually use

  def map[B](f: T => B): TraversableOnce[B] = new NSQWrappedValue[B](messageObject, underlying.map(f))

  def flatMap[B](f: T => GenTraversableOnce[B]): TraversableOnce[B] = new NSQWrappedValue[B](messageObject, underlying.flatMap(f))
}

object NSQWrappedValue {

  def factory[T](decodeFn : (Array[Byte]) => TraversableOnce[T])(message: NSQMessage): NSQWrappedValue[T] =
    new NSQWrappedValue[T](message, decodeFn(message.getMessage))

}
