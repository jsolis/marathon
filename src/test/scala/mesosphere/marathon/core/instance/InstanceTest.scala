package mesosphere.marathon.core.instance

import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance.InstanceState
import mesosphere.marathon.core.condition.Condition._
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.Timestamp
import org.scalatest.{ FunSuite, GivenWhenThen, Matchers }

class InstanceTest extends FunSuite with Matchers with GivenWhenThen {

  val stateChangeCases = Seq(
    (Created, Created, Seq(Created, Created, Created)),
    (Created, Staging, Seq(Created, Created, Staging)),
    (Staging, Staging, Seq(Running, Staging, Running)),
    (Running, Running, Seq(Running, Finished, Running)),
    (Running, Failed, Seq(Staging, Starting, Running, Killing, Finished, Failed)),
    (Running, Killing, Seq(Staging, Starting, Running, Killing, Finished)),
    (Running, Error, Seq(Staging, Starting, Running, Killing, Finished, Failed, Error)),
    (Staging, Staging, Seq(Staging)),
    (Running, Gone, Seq(Gone, Running, Running)),
    (Killing, Killed, Seq(Killed, Killed, Killed)),
    (Running, Killing, Seq(Running, Killing, Killed)),
    (Running, Gone, Seq(Running, Gone, Dropped)),
    (Running, Dropped, Seq(Unreachable, Dropped))
  )
  stateChangeCases.foreach { case (from, to, withTasks) =>
    test(s"State change from $from to $to with $withTasks is computed correctly") {
      Given(s"An instance in status $from with ${withTasks.size} Tasks in status $from")
      val (instance, tasks) = instanceWith(from, withTasks)

      When(s"The tasks become ${withTasks.mkString(", ")}")
      val status = Instance.newInstanceState(Some(instance.state), tasks, clock.now(), runSpecVersion = clock.now())

      Then(s"The status should be $to")
      status.status should be(to)
    }
  }

  val id = "/test".toPath
  val clock = ConstantClock()

  def instanceWith(condition: Condition, conditions: Seq[Condition]): (Instance, Map[Task.Id, Task]) = {
    def tasks(statuses: Seq[Condition]): Map[Task.Id, Task] = {

      statuses
        .map { status =>
          val task = TestTaskBuilder.Helper.minimalTask(Task.Id.forRunSpec(id), Timestamp.now(), None, status)
          task.taskId -> task
        }(collection.breakOut)
    }
    val state = InstanceState(condition, Timestamp.now(), None)
    val currentTasks = tasks(conditions.map(_ => condition))
    val newTasks = tasks(conditions)
    val instance = Instance(Instance.Id.forRunSpec(id), Instance.AgentInfo("", None, Nil), state, currentTasks, runSpecVersion = Timestamp.now())
    (instance, newTasks)
  }
}
