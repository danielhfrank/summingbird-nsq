import com.trendrr.nsq.NSQConsumer
import com.trendrr.nsq.lookup.NSQLookupDynMapImpl
import com.twitter.summingbird.TimeExtractor
import com.twitter.util.Try

class NSQPushSource[T: TimeExtractor](config: NSQClientConfig, decodeFn: Array[Byte] => Try[T]) {

  val lookup = new NSQLookupDynMapImpl
  config.lookupAddrs.foreach{case (host, port) => lookup.addAddr(host, port)}

  def buildNSQConsumer(receiver: Receiver[T]): NSQConsumer = {
    // first construct the message callback with the given receiver
    val callback = new NSQPushDriverCallback[T](receiver, decodeFn)
    // then construct a consumer using that callback
    val consumer = new NSQConsumer(lookup, config.topic, config.channel, callback)
    // TODO: at this point there are some reasonable settings that we should pull out of config
    // including dummy values right now
    consumer.setMessagesPerBatch(1)
    consumer.setLookupPeriod(30 * 1000)
    consumer
  }
}
