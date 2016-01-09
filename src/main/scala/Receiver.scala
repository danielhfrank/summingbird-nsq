import com.twitter.util.Future
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.batch.{Batcher, BatchID}
import com.twitter.storehaus.algebra.Mergeable

/**
 * Push based implementation similar to:
 * https://github.com/twitter/summingbird/blob/develop/summingbird-core/src/main/scala/com/twitter/summingbird/memory/ConcurrentMemory.scala
 */
sealed trait Receiver[-T] {
  def push(item: (Timestamp, T)): Future[Unit] = pushBatch(Iterator(item))
  def pushBatch(items: TraversableOnce[(Timestamp, T)]): Future[Unit] =
    items.foldLeft(Future.Unit) { (prev, t) => prev.join(push(t)).unit }
}

case class SourceReceiver[T](src: NSQPullSource[T], next: Receiver[T]) extends Receiver[Nothing] {
  override def push(item: (Timestamp, Nothing)) = sys.error("Cannot push into Source: " + item)
}

case class FlatMapReceiver[A, B](fn: A => TraversableOnce[B], next: Receiver[B]) extends Receiver[A] {
  override def pushBatch(items: TraversableOnce[(Timestamp, A)]) = Future {
    val bs: Iterator[(Timestamp, B)] = items.toIterator.flatMap { case (time, a) => fn(a).map((time, _)) }
    next.pushBatch(bs)
  }.flatten
}

case class StoreReciever[K, V](batcher: Batcher,
  ms: Mergeable[(K, BatchID), V],
  next: Receiver[(K, (Option[V], V))]) extends Receiver[(K, V)] {

  // todo implement this as pushBatch actually which can use multiMerge
  override def push(item: (Timestamp, (K, V))) = {
    val (time, (k, v)) = item
    val batch = batcher.batchOf(time)
    ms.merge(((k, batch), v))
      .map { optV => (time, (k, (optV, v))) }
      .flatMap(next.push)
  }
}

case class FanOut[T](nexts: Seq[Receiver[T]]) extends Receiver[T] {
  override def push(item: (Timestamp, T)) = Future.collect(nexts.map(_.push(item))).unit
}

case object NullReceiver extends Receiver[Any] {
  override def push(item: (Timestamp, Any)) = Future.Unit
  override def pushBatch(item: TraversableOnce[(Timestamp, Any)]) = Future.Unit
}
