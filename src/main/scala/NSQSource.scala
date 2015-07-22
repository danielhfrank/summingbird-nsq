import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, ConcurrentHashMap, ConcurrentLinkedQueue}

import com.trendrr.nsq.NSQMessage


class NSQSource[T](topic: String, channel: String, decodeFn : (Array[Byte]) => TraversableOnce[T]) {

  val queue = new LinkedBlockingQueue[NSQMessage]

  val msgIdMap = new ConcurrentHashMap[Array[Byte], NSQMessage]

  val wrappedValueFactory = NSQWrappedValue.factory(decodeFn) _

  private val killSwitch = new AtomicBoolean()

  def close() = killSwitch.set(true)

  def nextOption: Option[T]= {
    val nextElem = queue.poll(1001, TimeUnit.DAYS)
    val decoded = decodeFn(nextElem.getMessage)
    decoded.toList.headOption
  }

  def toStream: Stream[T] = {
    nextOption match {
      case Some(elem) => elem #:: toStream
      case None => toStream
    }
  }

  def nextWrappedValue: NSQWrappedValue[T] = {
    // TODO some kind of Future.select to shutdown?
    val nextElem = queue.poll(1001, TimeUnit.DAYS)
    wrappedValueFactory(nextElem)
  }

  def toWrappedStream: Stream[NSQWrappedValue[T]] =
    if(killSwitch.get()){
      Stream.empty[NSQWrappedValue[T]]
    } else{
      nextWrappedValue #:: toWrappedStream
    }


}
