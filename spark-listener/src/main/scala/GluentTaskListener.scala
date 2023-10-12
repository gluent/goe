import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.log4j.LogManager
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class GluentTaskListener extends SparkListener {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val logger = LogManager.getLogger("GluentTaskListener")
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
