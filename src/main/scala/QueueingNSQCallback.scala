import java.util

import com.trendrr.nsq.{NSQMessageCallback, NSQMessage}
import com.twitter.util.Future
import org.slf4j.{LoggerFactory, Logger}

class QueueingNSQCallback(queue: util.Queue[NSQMessage], maxAttempts: Int)
  extends NSQMessageCallback {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def error(x: Exception): Unit = {
    // fail harder
    val fs = Future.value(Seq("a", "b"))
//    Future.collect()
  }

  override def message(message: NSQMessage): Unit = {
    if(message.getAttempts > maxAttempts) {
      // TODO maybe write failed message to a file
      logger.info("Giving up on " + message.getId.toString)
      message.finished()
    } else {
      queue.add(message)
    }
  }
}
