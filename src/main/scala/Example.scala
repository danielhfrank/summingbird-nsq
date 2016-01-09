import com.twitter.algebird.Semigroup
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.Mergeable
import com.twitter.summingbird.online.MergeableStoreFactory
import com.twitter.summingbird.{TimeExtractor, Source}
import com.twitter.summingbird.batch.{Batcher, BatchID}
import com.twitter.util.Future

import scala.util.Try

import scala.language.existentials

object Example {

  private class FakeMergeableStore[V](override val semigroup: Semigroup[V]) extends ReadableStore[(String, BatchID), V]
    with Mergeable[(String, BatchID), V]{

    private val map = new scala.collection.mutable.HashMap[String, V]

    override def get(k: (String, BatchID)): Future[Option[V]] = Future.value(map.get(k._1))

    override def merge(kv: ((String, BatchID), V)): Future[Option[V]] = {
      val ((k, _), v) = kv
      val prevV = map.get(k)
      val newV = semigroup.sumOption(Seq(Some(v), prevV).flatten).get
      map.put(k, newV)
      Future.value(prevV)
    }
  }

  def storeFor[V](b: Batcher) = new NSQStore[String, V] {
    def batcher = b
    def mergeable(sg: Semigroup[V]): Mergeable[(String, BatchID), V] = new FakeMergeableStore(sg)
  }

  def printSink(sumResult: (String, (Option[Int], Int))): Future[Unit] = {
    val (k,(prevV, incrV)) = sumResult
    Future(println(s"k: $k, prevV: $prevV, incrV: $incrV"))
  }

  def main (args: Array[String]) {
    val clientConfig = NSQClientConfig("test", "summingbird-nsq", Seq(("localhost", 4161)))

    implicit val dummyTimeExtractor = TimeExtractor[String](_ => 0L)
    val batcher = Batcher.ofDays(1)

    val source = new NSQPullSource[String](clientConfig, bytes => Try(new String(bytes, "UTF-8")).toOption)
    val sbSource = Source[NSQ, String](source)
    val store = storeFor[Int](batcher)
    val mapped = sbSource.map{ s => (s, 1)}
    val summed = mapped.sumByKey(store)
    val written = summed.write(printSink)

    val stream = (new NSQ).plan(written)

    source.open()

    receiveOutputStream(stream)
  }

  def receiveOutputStream(out: NSQ#Plan[_]): Future[Unit] =
    out.run()
}
