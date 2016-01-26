import sbt._
import sbt.Keys._
import com.typesafe.sbt.pgp.PgpKeys._
import aether.AetherKeys._

object Publish {
  val snapshotsUrl = "https://nexus-content.stripe.com:446/repositories/snapshots"
  val releasesUrl = "https://nexus-content.stripe.com:446/repositories/releases"

  def getPublishTo(snapshot: Boolean) = {
    if (snapshot) {
      val url = sys.props.get("stripe.snapshots.url").getOrElse(snapshotsUrl)
      Some("stripe-nexus-snapshots" at url)
    } else {
      val url = sys.props.get("stripe.releases.url").getOrElse(releasesUrl)
      Some("stripe-nexus-releases" at url)
    }
  }

  lazy val settings = Seq(
    homepage := Some(url("http://github.com/stripe-internal/summingbird-nsq")),
    publishMavenStyle := true,
    publish := aetherDeploy.value,
    publishTo := getPublishTo(isSnapshot.value),
    publishArtifact in Test := false,
    pomIncludeRepository := Function.const(false),
    pomExtra := (
      <scm>
        <url>git@github.com:stripe-internal/summingbird-nsq.git</url>
        <connection>scm:git:git@github.com:stripe-internal/summingbird-nsq.git</connection>
        </scm>
        <developers>
        <developer>
        <name>Dan Frank</name>
        <email>df@stripe.com</email>
        <organization>Stripe</organization>
        <organizationUrl>https://stripe.com</organizationUrl>
          </developer>
        <developer>
        <name>Oscar Boykin</name>
        <email>oscar@stripe.com</email>
        <organization>Stripe</organization>
        <organizationUrl>https://stripe.com</organizationUrl>
          </developer>
        </developers>)
  )
}
