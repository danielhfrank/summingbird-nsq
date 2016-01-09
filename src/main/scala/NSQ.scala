import com.twitter.algebird.{Monoid, Semigroup}
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.algebra.{Mergeable, MergeableStore}
import com.twitter.summingbird._
import com.twitter.summingbird.graph._
import com.twitter.summingbird.batch.{Timestamp, BatchID, Batcher}
import com.twitter.summingbird.online.{OnlineServiceFactory, MergeableStoreFactory}
import com.twitter.summingbird.planner.DagOptimizer
import com.twitter.util.{Closable, Future, Time}

import scala.language.existentials

sealed trait NSQPlan {
  def run(): Future[Unit]
}

object NSQPlan {
  implicit def monoidNSQPlan: Monoid[NSQPlan] = new Monoid[NSQPlan] {
    val zero = new NSQPlan { def run() = Future.Unit }
    def plus(a: NSQPlan, b: NSQPlan) = new NSQPlan {
      def run() = a.run().join(b.run()).unit
    }
  }
}

case class SourcePlan[T](src: NSQPullSource[T], receiever: Receiver[T]) extends NSQPlan {
  def run() = sys.error("TODO: infinite loop of pulling from source and pushing into the receiver")
}

trait NSQStore[-K, V] {
  def batcher: Batcher
  def mergeable(sg: Semigroup[V]): Mergeable[(K, BatchID), V]
}

class NSQ extends Platform[NSQ]{

  // These can be taken from Memory
  type Sink[-T] = (T => Future[Unit])
  type Plan[T] = NSQPlan

  private type Prod[T] = Producer[NSQ, T]

  // These can be taken from Storm
  type Store[-K, V] = NSQStore[K, V]
  type Service[-K, +V] = ReadableStore[K, V]

  // And these are ours
  type Source[T] = NSQPullSource[T]

  /**
   * When planning a Producer[_, T] we create a Phys[T]
   * which gives the Receiver this Producer pushes into
   * as well as the Receiver for this Producer, whose
   * type we don't know (because Producer is not a category/arrow)
   */
  type Phys[T] = (Receiver[_], Receiver[T])
  /**
   * Given a Producer return the Receiver that all items should be pushed
   * onto
   */
  private def toPhys[T](deps: Dependants[NSQ],
    pushMap: HMap[Prod, Phys],
    that: Prod[T]): (HMap[Prod, Phys], Phys[T]) =
    pushMap.get(that) match {
      case Some(s) => (pushMap, s)
      case None =>
        // This is not type-safe, sadly...
        val (planned, target): (HMap[Prod, Phys], Receiver[T]) =
          deps.dependantsAfterMerge(that) match {
            case Nil => (pushMap, NullReceiver)
            case single :: Nil =>
              val (nextM, (r, _)) = toPhys(deps, pushMap, single)
              // since r single accepts form that, it receives T
              // but the type system won't help us here:
              (nextM, r.asInstanceOf[Receiver[T]])
            case many =>
              val res = many.scanLeft((pushMap, None: Option[Receiver[T]])) { case ((pm, _), p) =>
                val (post, (r, _)) = toPhys(deps, pm, p)
                // the dependants must be able to accept type T
                (post, Some(r.asInstanceOf[Receiver[T]]))
              }
              val finalMap = res.last._1
              val allDependants = res.collect { case (_, Some(r)) => r }
              (finalMap, FanOut[T](allDependants))
          }

        val thisR = that match {
          case Source(source) => SourceReceiver(source, target)
          case FlatMappedProducer(_, fn) => FlatMapReceiver(fn, target)
          case Summer(prod, store, sg) =>
            val mergeable = store.mergeable(sg)
            val batcher = store.batcher
            // Scala can't tell that T must be (K, (Option[V], V)) here
            // so we must cast
            def go[K, V](m: Mergeable[(K, BatchID), V]) =
              StoreReciever(batcher, m, target.asInstanceOf[Receiver[(K, (Option[V], V))]])

            go(mergeable)

          case WrittenProducer(prod, queue) => sys.error("unsupported: " + that)
          case LeftJoinedProducer(prod, service) => sys.error("unsupported: " + that)

          case other =>
            sys.error("%s encountered, which should have been optimized away".format(other))
        }
        val phys = (thisR, target)
        (planned + (that -> phys), phys)
    }

  def plan[T](prod: TailProducer[NSQ, T]): NSQPlan = {
    /*
     * These rules should normalize the graph into a plannable state by the toPhys
     * recursive function (removing no-op nodes and converting optionMap and KeyFlatMap to just
     * flatMap)
     */
    val dagOptimizer = new DagOptimizer[NSQ] {}
    import dagOptimizer._

    val ourRule = OptionToFlatMap
      .orElse(KeyFlatMapToFlatMap)
      .orElse(FlatMapFusion)
      .orElse(RemoveNames)
      .orElse(RemoveIdentityKeyed)
      .orElse(ValueFlatMapToFlatMap)


    val deps = Dependants(optimize(prod, ourRule))
    val heads: Seq[com.twitter.summingbird.Source[NSQ, _]] = deps.nodes.collect { case s @ Source(_) => s }
    // Now plan all the sources, and combine them:
    heads.foldLeft((HMap.empty[NSQ#Prod, Phys], Monoid.zero[NSQPlan])) {
      case ((hm, plan), head) =>
        // trick to get scala to see the types better (method with type parameter)
        def planSrc[T](s: com.twitter.summingbird.Source[NSQ, T]) = {
          val Source(nsqsrc) = s
          val (nextHm, (_, sourceR)) = toPhys(deps, hm, s)
          val srcPlan = SourcePlan(nsqsrc, sourceR)
          (nextHm, srcPlan)
        }
        val (nextHm, srcPlan) = planSrc(head)
        val nextPlan = Monoid.plus(plan, srcPlan)
        (nextHm, nextPlan)
    }._2
  }
}
