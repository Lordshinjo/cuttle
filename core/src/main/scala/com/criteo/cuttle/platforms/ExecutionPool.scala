package com.criteo.cuttle.platforms

import cats.effect.IO
import cats.syntax.semigroupk._
import io.circe._
import io.circe.syntax._
import org.http4s.HttpRoutes
import org.http4s.circe._
import org.http4s.dsl.io._

import scala.concurrent.stm._

/**
  * An execution pool backed by a priority queue. It limits the concurrent executions
  * and the priority queue is ordered by the [[scala.math.Ordering Ordering]] defined
  * on the [[com.criteo.cuttle.SchedulingContext SchedulingContext]].
  *
  * @param concurrencyLimit The maximum number of concurrent executions.
  */
case class ExecutionPool(concurrencyLimit: Int) extends WaitingExecutionQueue {
  def canRunNextCondition(implicit txn: InTxn) = _running().size < concurrencyLimit
  def doRunNext()(implicit txn: InTxn): Unit = ()

  override def routes(urlPrefix: Path): HttpRoutes[IO] =
    HttpRoutes.of[IO] {
      case GET -> `urlPrefix` =>
        Ok(
          Json.obj(
            "concurrencyLimit" -> concurrencyLimit.asJson,
            "running" -> running.size.asJson,
            "waiting" -> waiting.size.asJson
          )
        )
    } <+> super.routes(urlPrefix)
}
