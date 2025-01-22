/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "datasketches-spark"
version := "1.0-SNAPSHOT"
scalaVersion := "2.12.20"

organization := "org.apache.datasketches"
description := "The Apache DataSketches package for Spark"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

val sparkVersion = settingKey[String]("The version of Spark")
sparkVersion := sys.env.getOrElse("SPARK_VERSION", "3.5.4")

// determine our java version
val jvmVersionString = settingKey[String]("The JVM version")
jvmVersionString := sys.props("java.version")

// Map of JVM version prefix to:
// (JVM major version, datasketches-java version)
val jvmVersionMap = Map(
  "21" -> ("21", "8.0.0"),
  "17" -> ("17", "7.0.1"),
  "11" -> ("11", "6.2.0"),
  "8"  -> ("8",  "6.2.0"),
  "1.8" -> ("8", "6.2.0")
)

// determine the JVM major verison (default: 11)
val jvmVersion = settingKey[String]("The JVM major version")
jvmVersion := jvmVersionMap.collectFirst {
  case (prefix, (major, _)) if jvmVersionString.value.startsWith(prefix) => major
}.getOrElse("11")

// look up the associated datasketches-java version
val dsJavaVersion = settingKey[String]("The DataSketches Java version")
dsJavaVersion := jvmVersionMap.get(jvmVersion.value).map(_._2).get


javacOptions ++= Seq("-source", jvmVersion.value, "-target", jvmVersion.value)
scalacOptions ++= Seq("-encoding", "UTF-8", "-release", jvmVersion.value)
Test / javacOptions ++= Seq("-source", jvmVersion.value, "-target", jvmVersion.value)
Test / scalacOptions ++= Seq("-encoding", "UTF-8", "-release", jvmVersion.value)

libraryDependencies ++= Seq(
  "org.apache.datasketches" % "datasketches-java" % dsJavaVersion.value % "compile",
  "org.scala-lang" % "scala-library" % "2.12.6",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "org.scalatestplus" %% "junit-4-13" % "3.2.19.0" % "test"
)

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// additional options for java 17
Test / fork := {
  if (jvmVersion.value == "17") true
  else (Test / fork).value
}

Test / javaOptions ++= {
  if (jvmVersion.value == "17") {
    Seq("--add-modules=jdk.incubator.foreign",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  } else {
    Seq.empty
  }
}

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

Test / logBuffered := false

// Level.INFO is needed to see detailed output when running tests
Test / logLevel := Level.Info
