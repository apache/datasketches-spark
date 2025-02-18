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

import sbt._
import java.io.{BufferedWriter, FileWriter}
import scala.io.Source

object BuildUtils {

// Map of JVM version prefix to:
// (JVM major version, datasketches-java version)
// TODO: consider moving to external file
val jvmVersionMap = Map(
  "21" -> ("21", "8.0.0"),
  "17" -> ("17", "7.0.1"),
  "11" -> ("11", "6.2.0"),
  "8"  -> ("8",  "6.2.0"),
  "1.8" -> ("8", "6.2.0")
)

// TODO: any way to avoid hardcoding this?
val pythonVersionFileName = "python/src/datasketches_spark/version.txt"

// reads the version file, reformats as needed for python, and stores
// in the python subdirectory as the __version__ function for the package
def readVersionAndCopyToPython(filename: String): String = {
  val bufferedSource = Source.fromFile(filename)
  val version = try {
    bufferedSource.getLines.find(line => !line.trim.startsWith("#") && !line.trim.isEmpty()).get
  } finally {
    bufferedSource.close()
  }

  // write version to python subdirectory
  val pyVersion = version.replace("-SNAPSHOT", ".dev0")
  val writer = new BufferedWriter(new FileWriter(pythonVersionFileName))
  try {
    writer.write(pyVersion)
    writer.newLine()
  } finally {
    writer.close()
  }
  version
}

// removes the python version file from the python subdir
def cleanPythonVersionFile(): Unit = {
  val pyFile = new File(pythonVersionFileName)
  if (pyFile.exists()) {
    pyFile.delete()
  }
}

// copies the datasketches dependencies to a known location in /target
def copyDependenciesAndWriteList(targetLibDir: File, dependencies: Seq[Attributed[File]], listFile: File): Seq[File] = {
  IO.createDirectory(targetLibDir)
  val dependencyJars = dependencies.collect {
    case attr if (attr.data.getName.startsWith("datasketches-java") || attr.data.getName.startsWith("datasketches-memory"))
              && attr.data.getName.endsWith(".jar") =>
      val file = attr.data
      val targetFile = targetLibDir / file.getName
      IO.copyFile(file, targetFile)
      targetFile
  }

  // write list of copied jars to file so we have full names/versions
  val writer = new BufferedWriter(new FileWriter(listFile))
  try {
    dependencyJars.foreach { file =>
      writer.write(file.getName)
      writer.newLine()
    }
  } finally {
    writer.close()
  }

  dependencyJars
}

}
