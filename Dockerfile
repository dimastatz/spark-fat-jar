#
# Scala and sbt Dockerfile
#
# https://github.com/hseeberger/scala-sbt
#

# Pull base image
FROM openjdk:8u151

RUN wget -O- "http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.tgz" \
    | tar xzf - -C /usr/local --strip-components=1

Run wget -O- "https://github.com/sbt/sbt/releases/download/v0.13.17/sbt-0.13.17.tgz" \
    |  tar xzf - -C /usr/local --strip-components=1 \
    && sbt exit
