package com.criteo.cuttle.platforms.http

import java.util.concurrent.TimeUnit

import cats.effect.IO
import cats.syntax.semigroupk._
import com.criteo.cuttle._
import com.criteo.cuttle.platforms.{ExecutionPool, RateLimiter}
import io.circe._
import io.circe.syntax._
import org.http4s.circe._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.dsl.io._
import org.http4s.headers.Host
import org.http4s.{HttpRoutes, Request, Response}

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

/** Allow to make HTTP calls in a managed way with rate limiting. Globally the platform limits the number
  * of concurrent requests on the platform. Additionally a rate limiter must be defined for each host allowed
  * to be called by this platform.
  *
  * Example:
  * {{{
  *   platforms.http.HttpPlatform(
  *     maxConcurrentRequests = 10,
  *     rateLimits = Seq(
  *       .*[.]criteo[.](pre)?prod([:][0-9]+)?" -> platforms.http.HttpPlatform.RateLimit(100, per = SECONDS),
  *       google.com -> platforms.http.HttpPlatform.RateLimit(1, per = SECONDS)
  *     )
  *   ),
  * }}}
  *
  * While being rate limited, the [[com.criteo.cuttle.Job Job]] [[com.criteo.cuttle.Execution Execution]] is
  * seen as __WAITING__ in the UI.
  */
case class HttpPlatform(maxConcurrentRequests: Int, rateLimits: Seq[(String, HttpPlatform.RateLimit)])
    extends ExecutionPlatform {

  private[HttpPlatform] val pool = new ExecutionPool(concurrencyLimit = maxConcurrentRequests)
  private[HttpPlatform] val rateLimiters = rateLimits.map {
    case (pattern, HttpPlatform.RateLimit(tokens, per)) =>
      (pattern -> new RateLimiter(
        tokens,
        per match {
          case TimeUnit.DAYS    => (24 * 60 * 60 * 1000) / tokens
          case TimeUnit.HOURS   => (60 * 60 * 1000) / tokens
          case TimeUnit.MINUTES => (60 * 1000) / tokens
          case TimeUnit.SECONDS => (1000) / tokens
          case x                => sys.error(s"Non supported period, ${x}")
        }
      ))
  }

  override def waiting: Set[Execution[_]] =
    rateLimiters.map(_._2).foldLeft(pool.waiting)(_ ++ _.waiting)

  override lazy val publicRoutes: HttpRoutes[IO] =
    pool.routes(Root / "api" / "platforms" / "http" / "pool") <+> {
      val index = HttpRoutes.of[IO] {
        case GET -> Root / "api" / "platforms" / "http" / "rate-limiters" =>
          Ok(
            Json.obj(
              rateLimiters.zipWithIndex.map {
                case ((pattern, rateLimiter), i) =>
                  i.toString -> Json.obj(
                    "pattern" -> pattern.asJson,
                    "running" -> rateLimiter.running.size.asJson,
                    "waiting" -> rateLimiter.waiting.size.asJson
                  )
              }: _*
            )
          )
      }
      rateLimiters.zipWithIndex.foldLeft(index) {
        case (routes, ((_, rateLimiter), i)) =>
          routes <+> rateLimiter.routes(Root / "api" / "platforms" / "http" / "rate-limiters" / i.toString)
      }
    }
}

/** Access to the [[HttpPlatform]]. */
object HttpPlatform {

  /** A rate limiter for a given HTTP host. It uses a token bucket implementation.
    *
    * @param maxRequests Maximum number of requests allowed in the specified time slot.
    * @param per time slot.
    */
  case class RateLimit(maxRequests: Int, per: TimeUnit)

  /** Make an HTTP request via the platorm.
    *
    * @param request The [[Request]] to run.
    * @param thunk The function handling the HTTP resposne once received.
    */
  def request[A, S <: Scheduling](request: Request[IO], timeout: FiniteDuration = FiniteDuration(30, "seconds"))(
    thunk: Response[IO] => Future[A]
  )(implicit execution: Execution[S]): Future[A] = {
    val streams = execution.streams
    streams.debug(s"HTTP request: ${request}")
    val ec = implicitly[ExecutionContext]
    implicit val cs = IO.contextShift(ec)

    val httpPlatform =
      ExecutionPlatform.lookup[HttpPlatform].getOrElse(sys.error("No http execution platform configured"))
    httpPlatform.pool.run(execution, debug = request.toString) { () =>
      try {
        val host =
          request.headers.get(Host).getOrElse(sys.error("`Host' header must be present in the request")).value
        val rateLimiter = httpPlatform.rateLimiters
          .collectFirst {
            case (pattern, rateLimiter) if host.matches(pattern) =>
              rateLimiter
          }
          .getOrElse(sys.error(s"A rate limiter should be defined for `${host}'"))

        rateLimiter.run(execution, debug = request.toString) { () =>
          BlazeClientBuilder[IO](ec)
            .withRequestTimeout(timeout)
            .resource
            .use { client =>
              client.fetch(request) { resp =>
                IO.fromFuture(IO(thunk(resp)))
              }
            }
            .unsafeToFuture()
        }
      } catch {
        case e: Throwable =>
          Future.failed(e)
      }
    }
  }

}
