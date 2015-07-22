import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.{Mergeable, MergeableStore}
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{Timestamp, BatchID}
import com.twitter.summingbird.online.{OnlineServiceFactory, MergeableStoreFactory}
import com.twitter.summingbird.planner.DagOptimizer
import com.twitter.util.{Closable, Future, Time}


class NSQ extends Platform[NSQ]{

  // These can be taken from Memory
  type Sink[-T] = (T => Unit)
  type Plan[T] = Stream[NSQWrappedValue[T]]

  private type Prod[T] = Producer[NSQ, T]
  private type JamfMap = Map[Prod[_], Stream[NSQWrappedValue[_]]]

  // These can be taken from Storm
  type Store[-K, V] = Mergeable[K, V]
  type Service[-K, +V] = ReadableStore[K, V]

  // And these are ours
  type Source[T] = NSQSource[T]


  // -------------------

  def plan[T](prod: TailProducer[NSQ, T]): NSQ#Plan[T] = {
    val dagOptimizer = new DagOptimizer[NSQ] {}
    val memoryTail = dagOptimizer.optimize(prod, dagOptimizer.ValueFlatMapToFlatMap)
    val memoryDag = memoryTail.asInstanceOf[TailProducer[NSQ, T]]

    toStream(memoryDag, Map.empty)._1
  }

  def toStream[T, K, V](outerProducer: Prod[T], jamfs: JamfMap): (Stream[NSQWrappedValue[T]], JamfMap) = {
    jamfs.get(outerProducer) match {
      case Some(s) => (s.asInstanceOf[Stream[NSQWrappedValue[T]]], jamfs)
      case None =>
        val (s, m) = outerProducer match {
          case NamedProducer(producer, _) => toStream(producer, jamfs)
          case IdentityKeyedProducer(producer) => toStream(producer, jamfs)
          case Source(source) => (source.toWrappedStream, jamfs)
          case OptionMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.flatMap(fn(_)), m)

          case FlatMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.flatMap(fn(_)), m)

          case MergedProducer(l, r) =>
            val (leftS, leftM) = toStream(l, jamfs)
            val (rightS, rightM) = toStream(r, leftM)
            (leftS ++ rightS, rightM)

          case KeyFlatMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.map { streamValue =>
              streamValue.flatMapTrav {
                case (k, v) =>
                  fn(k).map((_, v))
              }
            }, m)

          case ValueFlatMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.map { streamValue =>
              streamValue.flatMapTrav {
                case (k, v) =>
                  fn(v).map((k, _))
              }
            }, m)

          case AlsoProducer(l, r) =>
            //Plan the first one, but ignore it
            val (left, leftM) = toStream(l, jamfs)
            // We need to force all of left to make sure any
            // side effects in write happen
            val lforcedEmpty = left.filter(_ => false)
            val (right, rightM) = toStream(r, leftM)
            (right ++ lforcedEmpty, rightM)

          case WrittenProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.map { i => fn(i); i}, m)

          case LeftJoinedProducer(producer, service) =>
            val (s, m) = toStream(producer, jamfs)
            val joined = s.map { streamValue =>
              streamValue.flatMapFut{
                case (k, v) =>
                  val futureJoinResult = service.get(k)
                  futureJoinResult.map { joinResult => (k, (v, joinResult))}
              }
            }
            (joined, m)

          case Summer(producer, store, monoid) =>
            // TODO - use online.executor.Summer
            val (s, m) = toStream(producer, jamfs)
            val summed = s.map { streamValue =>
              streamValue.flatMapFut {
                case pair@(k, deltaV) =>
                  val oldVFuture = store.merge(pair)
                  oldVFuture.map { oldV => (k, (oldV, deltaV))}
              }
            }
            (summed, m)
        }
        val goodS = s.asInstanceOf[Stream[NSQWrappedValue[T]]]
        (goodS, m + (outerProducer -> goodS))
    }
  }


}
