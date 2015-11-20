import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, ConcurrentHashMap, ConcurrentLinkedQueue}

import com.trendrr.nsq.{NSQConsumer, NSQMessage}
import com.trendrr.nsq.lookup.NSQLookupDynMapImpl


class NSQSource[T](config: NSQClientConfig, decodeFn : (Array[Byte]) => TraversableOnce[T]) {

  val queue = new LinkedBlockingQueue[NSQMessage]

  val wrappedValueFactory = NSQWrappedValue.factory(decodeFn) _

  private val killSwitch = new AtomicBoolean()

  val callback = new QueueingNSQCallback(queue, 1) // TODO configure me

  val lookup = new NSQLookupDynMapImpl
  config.lookupAddrs.foreach{case (host, port) => lookup.addAddr(host, port)}

  val consumer = new NSQConsumer(lookup, config.topic, config.channel, callback)
  consumer.setMessagesPerBatch(1)
  consumer.setLookupPeriod(60 * 1000)

  // ------------------------

  def close() = killSwitch.set(true)

  def nextWrappedValue: NSQWrappedValue[T] = {
    // TODO some kind of Future.select to shutdown?
    println("stuck in nextWrappedValue")
    val nextElem = queue.poll(1001, TimeUnit.DAYS)
    wrappedValueFactory(nextElem)
  }

  def toWrappedStream: Stream[NSQWrappedValue[T]] =
    if(killSwitch.get()){
      Stream.empty[NSQWrappedValue[T]]
    } else{
      println("in toWrappedStream")
//      Stream.newBuilder
      Stream.empty[NSQWrappedValue[T]] //#::: nextWrappedValue #:: toWrappedStream
    }

  def open() = {
    consumer.start()
  }


}
