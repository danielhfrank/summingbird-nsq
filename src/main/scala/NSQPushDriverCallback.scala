import com.trendrr.nsq.{NSQMessage, NSQMessageCallback}
import com.twitter.summingbird.TimeExtractor
import com.twitter.summingbird.batch.Timestamp
import com.twitter.util.Try

/**
  * This class handles receiving messages from nsqd, pushing them into a topology,
  * and ack/req-ing them when the topology is finished.
  *
  * Note - this implementation may create a number of outstanding Futures waiting to run onSuccess or onFailure.
  * We're relying on nsqd's rate-limiting (based on max-in-flight) to keep this number bounded
  */
class NSQPushDriverCallback[T: TimeExtractor](receiver: Receiver[T], decodeFn: (Array[Byte]) => Try[T]) extends NSQMessageCallback {

  val te = implicitly[TimeExtractor[T]]

  override def error(x: Exception): Unit = {}

  override def message(message: NSQMessage): Unit = {
    val tryMessageValue: Try[T] = decodeFn(message.getMessage)
    tryMessageValue.map { messageValue =>
      val timestamp = Timestamp(te(messageValue))
      val pushResult = receiver.push((timestamp, messageValue))
      pushResult
        .onSuccess(_ => message.finished())
        .onFailure(_ => message.requeue())
    }.onFailure { t => println("Could not decode " + new String(message.getMessage, "UTF-8")) }
  }
}
