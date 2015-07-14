import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.{Mergeable, MergeableStore}
import com.twitter.summingbird._
import com.twitter.summingbird.batch.{Timestamp, BatchID}
import com.twitter.summingbird.online.{OnlineServiceFactory, MergeableStoreFactory}
import com.twitter.util.{Closable, Future, Time}


trait NSQ extends Platform[NSQ]{

  // These can be taken from Memory
  type Sink[-T] = (T => Unit)
  type Plan[T] = Stream[T]

  private type Prod[T] = Producer[NSQ, T]
  private type JamfMap = Map[Prod[_], Stream[_]]

  // These can be taken from Storm
  type Store[-K, V] = Mergeable[(K, BatchID), V]
  type Service[-K, +V] = ReadableStore[K, V]

  // And these are ours
  type Source[T] = NSQSource[T]

  // -------------------


  // --------------------

  def toStream[T, K, V](outerProducer: Prod[T], jamfs: JamfMap): (Stream[Future[T]], JamfMap) = {
    val (s, m) = outerProducer match {
      case Summer(producer, store, monoid) =>
        // TODO - use online.executor.Summer
        val (s, m) = toStream(producer, jamfs)
        val summed = s.map { streamValue =>
           val futurePair = streamValue.asInstanceOf[Future[((K, BatchID),V)]]
           futurePair.flatMap {
            case pair@(k, deltaV) =>
              val oldVFuture = store.merge(pair)
              //            val oldV = store.get(k)
              //            val newV = oldV.map { monoid.plus(_, deltaV) }
              //              .getOrElse(deltaV)
              //            store.update(k, newV)
              oldVFuture.map { oldV => (k, (oldV, deltaV))}
          }
        }
        (summed, m)

      case Source(source) =>
        (source.toStream.map(Future.value), jamfs)

      case OptionMappedProducer(producer, fn) =>
        val (s, m) = toStream(producer, jamfs)
        (s.flatMap(fn(_)), m)

      case FlatMappedProducer(producer, fn) =>
        val (s, m) = toStream(producer, jamfs)
        (s.flatMap(fn(_)), m)
    }
    (s.asInstanceOf[Stream[Future[T]]], m + (outerProducer -> s))
  }


  // -------------------

//  def toStream[T, K, V](outerProducer: Prod[T], jamfs: JamfMap): (Stream[Future[T]], JamfMap)
//    jamfs.get(outerProducer) match {
//      case Some(s) => (s.asInstanceOf[Stream[T]], jamfs)
//      case None =>
//        val (s, m) = outerProducer match {
//          case NamedProducer(producer, _) => toStream(producer, jamfs)
//          case IdentityKeyedProducer(producer) => toStream(producer, jamfs)
//          case Source(source) => (source.toStream, jamfs)
//          case OptionMappedProducer(producer, fn) =>
//            val (s, m) = toStream(producer, jamfs)
//            (s.flatMap(fn(_)), m)
//
//          case FlatMappedProducer(producer, fn) =>
//            val (s, m) = toStream(producer, jamfs)
//            (s.flatMap(fn(_)), m)
//
//          case MergedProducer(l, r) =>
//            val (leftS, leftM) = toStream(l, jamfs)
//            val (rightS, rightM) = toStream(r, leftM)
//            (leftS ++ rightS, rightM)
//
//          case KeyFlatMappedProducer(producer, fn) =>
//            val (s, m) = toStream(producer, jamfs)
//            (s.flatMap {
//              case (k, v) =>
//                fn(k).map((_, v))
//            }, m)
//
//          case AlsoProducer(l, r) =>
//            //Plan the first one, but ignore it
//            val (left, leftM) = toStream(l, jamfs)
//            // We need to force all of left to make sure any
//            // side effects in write happen
//            val lforcedEmpty = left.filter(_ => false)
//            val (right, rightM) = toStream(r, leftM)
//            (right ++ lforcedEmpty, rightM)
//
//          case WrittenProducer(producer, fn) =>
//            val (s, m) = toStream(producer, jamfs)
//            (s.map { i => fn(i); i }, m)
//
//          case LeftJoinedProducer(producer, service) =>
//            val (s, m) = toStream(producer, jamfs)
//            val joined = s.map {
//              case (k, v) => (k, (v, service.get(k)))
//            }
//            (joined, m)
//
//          case Summer(producer, store, monoid) =>
//            // TODO - use online.executor.Summer
//            val (s, m) = toStream(producer, jamfs)
//            val summed = s.map {
//              case pair @ (k, deltaV) =>
//                val oldV = store.get(k)
//                val newV = oldV.map { monoid.plus(_, deltaV) }
//                  .getOrElse(deltaV)
//                store.update(k, newV)
//                (k, (oldV, deltaV))
//            }
//            (summed, m)
//        }
//        (s.asInstanceOf[Stream[T]], m + (outerProducer -> s))
//    }
//

}
