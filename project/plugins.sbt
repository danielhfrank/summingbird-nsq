resolvers ++= Seq(
  "sonatype-releases"  at "http://oss.sonatype.org/content/repositories/releases"
)

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.0-RC1")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.4")
addSbtPlugin("no.arktekk.sbt" % "aether-deploy" % "0.14")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
