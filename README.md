## summingbird-nsq

`summingbird-nsq` is a library that allows [summingbird](https://github.com/twitter/summingbird) to process events sourced from [NSQ](http://nsq.io/).


## Compiling and Testing

[The SBT hackpad](https://hackpad.corp.stripe.com/SBT-BuildingPublishing-mXO5OxVG7es) has some basic information on SBT.

The example code's not going to work for you, unless you've got @danielhfrank's NSQ setup. If you do, run this command to invoke [Example.scala](https://github.com/danielhfrank/summingbird-nsq/blob/master/src/main/scala/com/stripe/summingbird/nsq/Example.scala):

```scala
./sbt "run-main com.stripe.summingbird.nsq.Example"
```

Good luck!

## Deploying

To deploy summingbird-nsq internally at Stripe, first follow the instructions at the [Nexus Nexus hackpad](https://hackpad.corp.stripe.com/The-Nexus-Nexus-3gUoWozbmSx#:h=Deployment-Credentials-for-SBT) to get your credentials all set up. Once that's complete, just run

```scala
./sbt release
```

The release plugin will bump versions, deploy to our internal Nexus and get all the little maintenance commits pushed up the main repository.
