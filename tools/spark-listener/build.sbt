/*
# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/

ThisBuild / organization := "com.goe"
ThisBuild / scalaVersion := "2.12.14"
ThisBuild / version      := "1.0"

val sparkVersion = sys.props.getOrElse("sparkVersion", "3.2.0")

lazy val root = (project in file("."))
  .settings(
    name := "spark-" + sparkVersion + "-listener",
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      "spark-" + sparkVersion + "-listener" + "_" + sv.binary +  "-" + module.revision + "." + artifact.extension
    },
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
  )
