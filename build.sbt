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

// these do not impact code generation in spark
javacOptions ++= Seq("-source", "17", "-target", "17")
scalacOptions ++= Seq("-encoding", "UTF-8", "-release", "17")
Test / javacOptions ++= Seq("-source", "17", "-target", "17")
Test / scalacOptions ++= Seq("-encoding", "UTF-8", "-release", "17")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.6",
  "org.apache.spark" %% "spark-sql" % "3.5.4" % "provided",
  "org.apache.datasketches" % "datasketches-java" % "7.0.0" % "compile",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "org.scalatestplus" %% "junit-4-13" % "3.2.19.0" % "test"
)

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// for java 17
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-modules=jdk.incubator.foreign",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

Test / logBuffered := false

// Level.INFO is needed to see detailed output when running tests
Test / logLevel := Level.Info
