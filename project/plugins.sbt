// You may use this file to add plugin dependencies for sbt.

resolvers += "JBoss" at "https://repository.jboss.org/"

resolvers += "Spark Packages repo" at "https://dl.bintray.com/spark-packages/maven/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")