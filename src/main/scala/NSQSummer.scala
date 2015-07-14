import com.twitter.algebird.Semigroup
import com.twitter.algebird.util.summer.Incrementor
import com.twitter.summingbird.storm.Constants._
import com.twitter.summingbird.storm.{SingleItemInjection, KeyValueInjection}
import com.twitter.summingbird.{Counter, Name, Group, Summer}
import com.twitter.summingbird.batch.{Timestamp, BatchID}
import com.twitter.summingbird.online.option.{IncludeSuccessHandler, SummerBuilder}
import com.twitter.summingbird.online.{executor, FlatMapOperation, MergeableStoreFactoryAlgebra, MergeableStoreFactory}
import com.twitter.summingbird.option.JobId

import scala.collection.Map

object NSQSummer {
//
//  def getExecSummer[K,V](name: String, summer: Summer[NSQ, K, V]) = {
//    implicit val semigroup = summer.semigroup
//    implicit val batcher = summer.store.mergeableBatcher
//
//    type ExecutorKeyType = (K, BatchID)
//    type ExecutorValueType = (Timestamp, V)
//    type ExecutorOutputType = (Timestamp, (K, (Option[V], V)))
//
//    val supplier: MergeableStoreFactory[ExecutorKeyType, V] = summer.store match {
//      case m: MergeableStoreFactory[ExecutorKeyType, V] => m
//      case _ => sys.error("Should never be able to get here, looking for a MergeableStoreFactory from %s".format(summer.store))
//    }
//
//    val wrappedStore: MergeableStoreFactory[ExecutorKeyType, ExecutorValueType] =
//      MergeableStoreFactoryAlgebra.wrapOnlineFactory(supplier)
//
//    // TODO hacking together as little as can be done
//    val tupleInCounter = counter(JobId(name), Group(name), Name("tuplesIn"))
//    val tupleOutCounter = counter(JobId(name), Group(name), Name("tuplesOut"))
//
//    val builder = new SummerBuilder {
//      // TODO - I've just copied in the form that does no client-side caching
//      def getSummer[K, V: Semigroup]: com.twitter.algebird.util.summer.AsyncSummer[(K, V), Map[K, V]] = {
//        new com.twitter.algebird.util.summer.NullSummer[K, V](tupleInCounter, tupleOutCounter)
//      }
//    }
//
//    val storeBaseFMOp = { op: (ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)) =>
//      val ((k, batchID), (optiVWithTS, (ts, v))) = op
//      val optiV = optiVWithTS.map(_._2)
//      List((ts, (k, (optiV, v))))
//    }
//
//    val flatmapOp: FlatMapOperation[(ExecutorKeyType, (Option[ExecutorValueType], ExecutorValueType)), ExecutorOutputType] =
//      FlatMapOperation.apply(storeBaseFMOp)
//
//    new executor.Summer(
//      wrappedStore,
//      flatmapOp,
//      DEFAULT_ONLINE_SUCCESS_HANDLER,
//      DEFAULT_ONLINE_EXCEPTION_HANDLER,
//      builder,
//      DEFAULT_MAX_WAITING_FUTURES,
//      DEFAULT_MAX_FUTURE_WAIT_TIME,
//      DEFAULT_MAX_EMIT_PER_EXECUTE,
//      IncludeSuccessHandler.default,
//      new KeyValueInjection[Int, Map[ExecutorKeyType, ExecutorValueType]],
//      new SingleItemInjection[ExecutorOutputType])
//  }
//
//  def counter(jobID: JobId, nodeName: Group, counterName: Name) = new Counter(Group("summingbird." + nodeName.getString), counterName)(jobID) with Incrementor


}
