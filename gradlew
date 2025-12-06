#!/usr/bin/env sh

set -e

PRG="$0"
while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \\(.*\\)$')
  if expr "$link" : '/.*' >/dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done

SAVED_PWD=$(pwd)
cd "$(dirname "$PRG")" >/dev/null
APP_HOME=$(pwd -P)
cd "$SAVED_PWD" >/dev/null

CLASSPATH="$APP_HOME/gradle/wrapper/gradle-wrapper.jar"

JAVA_EXE=java
if [ -n "$JAVA_HOME" ]; then
  JAVA_EXE="$JAVA_HOME/bin/java"
  if [ ! -x "$JAVA_EXE" ]; then
    echo "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME" >&2
    exit 1
  fi
fi

exec "$JAVA_EXE" ${JAVA_OPTS:-} ${GRADLE_OPTS:-} -Dorg.gradle.appname=gradlew -classpath "$CLASSPATH" org.gradle.wrapper.GradleWrapperMain "$@"
