import com.twitter.summingbird.TimeExtractor
import com.twitter.util.Future

class NSQPushPlan[T: TimeExtractor](sourceRecPairs: Seq[(NSQPushSource[T], Receiver[T])]) {

  val consumers = sourceRecPairs.map{
    case (source, receiver) => source.buildNSQConsumer(receiver)
  }

  def run(): Future[Unit] = {
    // Consumer.start simply opens up connections, background threads do the work
    // TODO await until close called or something
    Future(consumers.foreach(_.start()))
  }

}
