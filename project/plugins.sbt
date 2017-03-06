resolvers += "JBoss" at "https://repository.jboss.org/"

resolvers += "Spark Packages repo" at "https://dl.bintray.com/spark-packages/maven/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")