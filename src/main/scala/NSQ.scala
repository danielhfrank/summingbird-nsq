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
  type Plan[T] = Stream[Future[TraversableOnce[T]]]

  private type Prod[T] = Producer[NSQ, T]
  private type JamfMap = Map[Prod[_], Stream[Future[TraversableOnce[_]]]]

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

  def toStream[T, K, V](outerProducer: Prod[T], jamfs: JamfMap): (Stream[Future[TraversableOnce[T]]], JamfMap) = {
    jamfs.get(outerProducer) match {
      case Some(s) => (s.asInstanceOf[Stream[Future[TraversableOnce[T]]]], jamfs)
      case None =>
        val (s, m) = outerProducer match {
          case NamedProducer(producer, _) => toStream(producer, jamfs)
          case IdentityKeyedProducer(producer) => toStream(producer, jamfs)
          case Source(source) => (source.toWrappedStream.map(x => Future.value(x)), jamfs)
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
            (s.map { futureStreamValue =>
              futureStreamValue.map { streamValue =>
                streamValue.flatMap {
                  case (k, v) =>
                    fn(k).map((_, v))
                }
              }
            }, m)

          case ValueFlatMappedProducer(producer, fn) =>
            val (s, m) = toStream(producer, jamfs)
            (s.map { futureStreamValue =>
              futureStreamValue.map { streamValue =>
                streamValue.flatMap {
                  case (k, v) =>
                    fn(v).map((k, _))
                }
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
            val joined = s.map { futureStreamValue =>
              futureStreamValue.flatMap { streamValue =>
                Future.collect(streamValue.map {
                  case (k, v) =>
                    val futureJoinResult = service.get(k)
                    futureJoinResult.map { joinResult => (k, (v, joinResult))}
                }.toList)
              }
            }
            (joined, m)

          case Summer(producer, store, monoid) =>
            // TODO - use online.executor.Summer
            val (s, m) = toStream(producer, jamfs)
            val summed = s.map { futureStreamValue =>
              futureStreamValue.flatMap { maybePair =>
                val maybeFutureResult = maybePair.map { // TODO match on wrapped value here and inject exception?
                  case pair@(k, deltaV) =>
                    val oldVFuture = store.merge(pair)
                    oldVFuture.map { oldV => (k, (oldV, deltaV))}
                }
                Future.collect(maybeFutureResult.toList)
              }

            }
            (summed, m)
        }
        val goodS = s.asInstanceOf[Stream[Future[TraversableOnce[T]]]]
        (goodS, m + (outerProducer -> goodS))
    }
  }


}
