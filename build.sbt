val stripeResolver = "Internal Stripe Nexus" at "https://nexus-content.corp.stripe.com:446/groups/public"

lazy val commonSettings = Seq(
  organization := "com.stripe",
  scalaVersion := "2.11.7",
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-optimize"
  ),
  maxErrors := 8,
  resolvers ++= Seq(
    stripeResolver,
    Opts.resolver.sonatypeSnapshots,
    Opts.resolver.sonatypeReleases
  ),
  autoAPIMappings := true
)

// Versions:
val summingbirdVersion = "0.9.1"
val scalaCheckVersion = "1.12.2"
val scalatestVersion = "2.2.4"

def summingbird(module: String) =
  "com.twitter" %% "summingbird-%s".format(module) % summingbirdVersion

//
lazy val scalacheck = "org.scalacheck" %% "scalacheck" % "1.12.0"

lazy val root = project.in(file(".")).
  settings(commonSettings: _*).
  settings(Publish.settings: _*).
  settings(
    name := "summingbird-nsq",
    libraryDependencies ++= Seq(
      summingbird("core"),
      summingbird("online"),
      summingbird("storm"),
      "com.github.dustismo" % "trendrr-nsq-client" % "1.4-SNAPSHOT",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
