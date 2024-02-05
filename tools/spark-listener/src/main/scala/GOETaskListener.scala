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

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.log4j.LogManager
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class GOETaskListener extends SparkListener {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val logger = LogManager.getLogger("GOETaskListener")
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics
    val launchTime = taskInfo.launchTime
    val finishTime = taskInfo.finishTime
    val duration = finishTime - launchTime
    val recordsWritten = taskMetrics.outputMetrics.recordsWritten
    val executorRunTime = taskMetrics.executorRunTime
    val message = compact(render(
      ("taskInfo.id" ->  taskInfo.id) ~
      ("taskInfo.taskId" ->  taskInfo.taskId) ~
      ("taskInfo.launchTime" ->  taskInfo.launchTime) ~
      ("taskInfo.finishTime" ->  taskInfo.finishTime) ~
      ("duration" ->  duration) ~
      ("recordsWritten" ->  recordsWritten) ~
      ("executorRunTime" -> executorRunTime)
    ))
    logger.info(message)
  }
}
