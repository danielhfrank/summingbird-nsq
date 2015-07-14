import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, ConcurrentHashMap, ConcurrentLinkedQueue}

import com.trendrr.nsq.NSQMessage


class NSQSource[+T](topic: String, channel: String, decodeFn : (Array[Byte]) => TraversableOnce[T]) {

  val queue = new LinkedBlockingQueue[NSQMessage]

  val msgIdMap = new ConcurrentHashMap[Array[Byte], NSQMessage]

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


}
