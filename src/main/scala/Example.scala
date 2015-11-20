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

  private class FakeMergeableStore extends ReadableStore[(String, BatchID), Int] with Mergeable[(String, BatchID), Int]{

    private val intMap = new scala.collection.mutable.HashMap[String, Int]

    override def get(k: (String, BatchID)): Future[Option[Int]] = Future.value(intMap.get(k._1))

    override def semigroup: Semigroup[Int] = Semigroup.intSemigroup

    override def merge(kv: ((String, BatchID), Int)): Future[Option[Int]] = {
      val ((k, _),v) = kv
      val prevV = intMap.get(k)
      val newV = semigroup.sumOption(Seq(Some(v), prevV).flatten).get
      intMap.put(k, newV)
      Future.value(prevV)
    }
  }

  private val ourStore = new FakeMergeableStore
  def storeFor(batcher: Batcher) = {
    MergeableStoreFactory({ () => ourStore}, batcher)
  }

  def printSink(sumResult: (String, (Option[Int], Int))): Unit = {
    val (k,(prevV, incrV)) = sumResult
    println(s"k: $k, prevV: $prevV, incrV: $incrV")
  }

  def main (args: Array[String]) {
    val clientConfig = NSQClientConfig("test", "summingbird-nsq", Seq(("localhost", 4161)))

    implicit val dummyTimeExtractor = TimeExtractor[String](_ => 0L)
    val batcher = Batcher.ofDays(1)

    val source = new NSQSource[String](clientConfig, bytes => Try(new String(bytes, "UTF-8")).toOption)
    val sbSource = Source[NSQ, String](source)
    val store = storeFor(batcher)
    val mapped = sbSource.map{ s => (s, 1)}
    val summed = mapped.sumByKey(store)
    val written = summed.write(printSink)

    val stream = (new NSQ).plan(written)

    source.open()

    receiveOutputStream(stream)
  }

  def receiveOutputStream(out: NSQ#Plan[_]) = {
    out.foreach{
        case NSQWrappedValue(msg, _, result) =>
          result
            .onSuccess(_ => msg.foreach(_.finished()))
            .onFailure(_ => msg.foreach(_.requeue()))
    }
  }

}
