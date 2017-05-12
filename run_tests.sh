set -e
sbt scalastyle
sbt assembly
sbt test