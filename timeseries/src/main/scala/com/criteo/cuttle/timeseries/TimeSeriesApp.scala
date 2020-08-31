package com.criteo.cuttle.timeseries

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import scala.util._
import cats.effect.IO
import cats.implicits._
import doobie.implicits._
import io.circe._
import io.circe.generic.auto._
import io.circe.java8.time._
import io.circe.syntax._
import org.http4s.{headers, HttpApp, HttpRoutes, MediaType, Request, Response, ServerSentEvent, StaticFile}
import org.http4s.dsl.io._
import org.http4s.circe._
import org.http4s.syntax.kleisli._
import com.criteo.cuttle.ExecutionStatus._
import com.criteo.cuttle.Metrics.{Gauge, Prometheus}
import com.criteo.cuttle._
import com.criteo.cuttle.ThreadPools._
import com.criteo.cuttle.utils.getJVMUptime
import com.criteo.cuttle.events.JobSuccessForced
import com.criteo.cuttle.timeseries.TimeSeriesUtils._
import com.criteo.cuttle.timeseries.intervals.Bound.{Bottom, Finite, Top}
import com.criteo.cuttle.timeseries.intervals._
import com.criteo.cuttle.utils.sse

private[timeseries] object TimeSeriesApp {

  implicit def projectEncoder: Encoder[CuttleProject] = new Encoder[CuttleProject] {
    override def apply(project: CuttleProject): Json =
      Json.obj(
        "name" -> project.name.asJson,
        "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
        "description" -> Option(project.description)
          .filterNot(_.isEmpty)
          .asJson,
        "scheduler" -> "timeseries".asJson,
        "env" -> Json.obj(
          "name" -> Option(project.env._1).filterNot(_.isEmpty).asJson,
          "critical" -> project.env._2.asJson
        )
      )
  }

  implicit val intervalEncoder = new Encoder[Interval[Instant]] {
    implicit val boundEncoder = new Encoder[Bound[Instant]] {
      override def apply(bound: Bound[Instant]) = bound match {
        case Bottom    => "-oo".asJson
        case Top       => "+oo".asJson
        case Finite(t) => t.asJson
      }
    }

    override def apply(interval: Interval[Instant]) =
      Json.obj("start" -> interval.lo.asJson, "end" -> interval.hi.asJson)
  }

  implicit val executionPeriodEncoder = new Encoder[ExecutionPeriod] {
    override def apply(executionPeriod: ExecutionPeriod) = {
      val coreFields = List(
        "period" -> executionPeriod.period.asJson,
        "backfill" -> executionPeriod.backfill.asJson,
        "aggregated" -> executionPeriod.aggregated.asJson,
        "version" -> executionPeriod.version.asJson
      )
      val finalFields = executionPeriod match {
        case JobExecution(_, status, _, _) =>
          ("status" -> status.asJson) :: coreFields
        case AggregatedJobExecution(_, completion, error, _, _) =>
          ("completion" -> completion.asJson) :: ("error" -> error.asJson) :: coreFields
      }
      Json.obj(finalFields: _*)
    }
  }

  case class JobTimeline(jobId: String, calendarView: TimeSeriesCalendarView, executions: List[ExecutionPeriod])

  implicit val jobTimelineEncoder = new Encoder[JobTimeline] {
    override def apply(jobTimeline: JobTimeline) = jobTimeline.executions.asJson
  }
}

private[timeseries] case class TimeSeriesApp(project: CuttleProject,
                                             executor: Executor[TimeSeries],
                                             scheduler: TimeSeriesScheduler,
                                             xa: XA) {

  import project.{jobs}

  import JobState._
  import TimeSeriesApp._
  import TimeSeriesCalendar._

  private val allIds = jobs.all.map(_.id)

  private def parseJobIds(jobsQueryString: String): Set[String] =
    jobsQueryString.split(",").filter(_.trim().nonEmpty).toSet

  private def getJobsOrNotFound(
    jobsQueryString: String
  ): Either[IO[Response[IO]], Set[Job[TimeSeries]]] = {
    val jobsNames = parseJobIds(jobsQueryString)
    if (jobsNames.isEmpty) Right(jobs.all)
    else {
      val filteredJobs = jobs.all.filter(v => jobsNames.contains(v.id))
      if (filteredJobs.isEmpty) Left(NotFound())
      else Right(filteredJobs)
    }
  }

  private object EventsParam extends OptionalQueryParamDecoderMatcher[String]("events")
  private object JobsParam extends OptionalQueryParamDecoderMatcher[String]("jobs")
  private object JobParam extends OptionalQueryParamDecoderMatcher[String]("job")
  private object StartParam extends OptionalQueryParamDecoderMatcher[String]("start")
  private object EndParam extends OptionalQueryParamDecoderMatcher[String]("end")
  private object GracePeriodParam extends OptionalQueryParamDecoderMatcher[Long]("gracePeriodSeconds")
  private object HardParam extends OptionalQueryParamDecoderMatcher[String]("hard")

  private def reqSupportsEventStream(request: Request[IO]): Boolean =
    request.headers.get(headers.Accept) match {
      case Some(h) if h.values.exists(_.mediaRange == MediaType.`text/event-stream`) => true
      case _                                                                         => false
    }

  val publicApi: HttpRoutes[IO] = HttpRoutes.of[IO] {

    case GET -> Root / "api" / "status" =>
      val projectJson = (status: String) =>
        Json.obj(
          "project" -> project.name.asJson,
          "version" -> Option(project.version).filterNot(_.isEmpty).asJson,
          "status" -> status.asJson
        )
      executor.healthCheck() match {
        case Success(_) => Ok(projectJson("ok"))
        case _          => InternalServerError(projectJson("ko"))
      }

    case request @ POST -> Root / "api" / "statistics" =>
      def getStats(ids: Set[String]): IO[Option[(Json, Json)]] =
        executor
          .getStats(ids)
          .map(stats => Try(stats -> scheduler.getStats(ids)).toOption)

      def asJson(x: (Json, Json)) = x match {
        case (executorStats, schedulerStats) =>
          executorStats.deepMerge(Json.obj("scheduler" -> schedulerStats))
      }
      request
        .as[Json]
        .flatMap { json =>
          json.hcursor
            .downField("jobs")
            .as[Set[String]]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              jobIds => {
                val ids = if (jobIds.isEmpty) allIds else jobIds
                getStats(ids).flatMap(
                  _.map(stat => Ok(asJson(stat))).getOrElse(InternalServerError())
                )
              }
            )
        }

    case GET -> Root / "api" / "statistics" / jobName =>
      executor
        .jobStatsForLastThirtyDays(jobName)
        .flatMap(stats => Ok(stats.asJson))

    case GET -> Root / "version" => Ok(project.version)

    case GET -> Root / "metrics" =>
      val metrics =
        executor.getMetrics(allIds, jobs) ++
          scheduler.getMetrics(allIds, jobs) :+
          Gauge("cuttle_jvm_uptime_seconds")
            .labeled(("version", project.version), getJVMUptime)
      Ok(Prometheus.serialize(metrics))

    case request @ POST -> Root / "api" / "executions" / "status" / kind => {
      def getExecutions(
        q: ExecutionsQuery
      ): IO[Option[(Int, List[ExecutionLog])]] = kind match {
        case "started" =>
          IO(
            Some(
              executor.runningExecutionsSizeTotal(q.jobIds(allIds)) -> executor
                .runningExecutions(
                  q.jobIds(allIds),
                  q.sort.column,
                  q.sort.asc,
                  q.offset,
                  q.limit
                )
                .toList
            )
          )
        case "stuck" =>
          IO(
            Some(
              executor.failingExecutionsSize(q.jobIds(allIds)) -> executor
                .failingExecutions(
                  q.jobIds(allIds),
                  q.sort.column,
                  q.sort.asc,
                  q.offset,
                  q.limit
                )
                .toList
            )
          )
        case "finished" =>
          executor
            .archivedExecutionsSize(q.jobIds(allIds))
            .map(ids => Some(ids -> executor.allRunning.toList))
        case _ =>
          IO.pure(None)
      }

      def asJson(q: ExecutionsQuery, x: (Int, Seq[ExecutionLog])): IO[Json] =
        x match {
          case (total, executions) =>
            (kind match {
              case "finished" =>
                executor
                  .archivedExecutions(
                    scheduler.allContexts,
                    q.jobIds(allIds),
                    q.sort.column,
                    q.sort.asc,
                    q.offset,
                    q.limit
                  )
                  .map(execs => execs.asJson)
              case _ =>
                IO(executions.asJson)
            }).map(
              data =>
                Json.obj(
                  "total" -> total.asJson,
                  "offset" -> q.offset.asJson,
                  "limit" -> q.limit.asJson,
                  "sort" -> q.sort.asJson,
                  "data" -> data
                )
            )
        }

      request
        .as[Json]
        .flatMap { json =>
          json
            .as[ExecutionsQuery]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              query => {
                getExecutions(query)
                  .flatMap(
                    _.map(e => asJson(query, e).flatMap(json => Ok(json)))
                      .getOrElse(NotFound())
                  )
              }
            )
        }
    }

    case GET -> Root / "api" / "executions" / id :? EventsParam(events) =>
      def getExecution =
        IO.suspend(executor.getExecution(scheduler.allContexts, id))

      events match {
        case Some("true" | "yes") =>
          sse(getExecution, (e: ExecutionLog) => IO(e.asJson))
        case _ =>
          getExecution.flatMap(_.map(e => Ok(e.asJson)).getOrElse(NotFound()))
      }

    case req @ GET -> Root / "api" / "executions" / id / "streams" =>
      lazy val streams = executor.openStreams(id)
      if (reqSupportsEventStream(req))
        Ok(
          fs2.Stream(ServerSentEvent("BOS")) ++
            streams
              .through(fs2.text.utf8Decode)
              .through(fs2.text.lines)
              .chunks
              .map(
                chunk =>
                  ServerSentEvent(
                    Json.fromValues(chunk.toArray.toIterable.map(_.asJson)).noSpaces
                  )
              ) ++
            fs2.Stream(ServerSentEvent("EOS"))
        )
      else
        Ok(streams).map(_.withHeaders(headers.`Content-Type`(MediaType.text.plain)))

    case GET -> Root / "api" / "jobs" / "paused" =>
      Ok(scheduler.pausedJobs().asJson)

    case GET -> Root / "api" / "project_definition" =>
      Ok(project.asJson)

    case GET -> Root / "api" / "jobs_definition" =>
      Ok(jobs.asJson)
  }

  val privateApi: Auth.Routes = Auth.Routes.of {
    case POST -> Root / "api" / "executions" / id / "cancel" => { implicit user =>
      executor.cancelExecution(id)
      Ok()
    }

    case request @ POST -> Root / "api" / "jobs" / "pause" => { implicit user =>
      request.as[Json].flatMap { json =>
        val jobs = json.hcursor.get[String]("jobs").getOrElse("")
        getJobsOrNotFound(jobs).fold(identity, jobs => {
          scheduler.pauseJobs(jobs, executor, xa)
          Ok()
        })
      }
    }

    case request @ POST -> Root / "api" / "jobs" / "resume" => { implicit user =>
      request.as[Json].flatMap { json =>
        val jobs = json.hcursor.get[String]("jobs").getOrElse("")
        getJobsOrNotFound(jobs).fold(identity, jobs => {
          scheduler.resumeJobs(jobs, xa)
          Ok()
        })
      }
    }
    case POST -> Root / "api" / "jobs" / "all" / "unpause" => { implicit user =>
      scheduler.resumeJobs(jobs.all, xa)
      Ok()
    }
    case POST -> Root / "api" / "jobs" / id / "unpause" => { implicit user =>
      jobs.all.find(_.id == id).fold(NotFound()) { job =>
        scheduler.resumeJobs(Set(job), xa)
        Ok()
      }
    }
    case req @ POST -> Root / "api" / "executions" / "relaunch" :? JobsParam(jobs) => { implicit user =>
      val filteredJobs = Try(jobs.toSeq.flatMap(_.split(",")).filter(_.nonEmpty)).toOption
        .filter(_.nonEmpty)
        .getOrElse(allIds)
        .toSet

      executor.relaunch(filteredJobs)
      Ok()
    }

    case req @ GET -> Root / "api" / "shutdown" :? GracePeriodParam(grace) :? HardParam(hard) => { implicit user =>
      import scala.concurrent.duration._

      (grace, hard) match {
        case (Some(s), _) if s > 0 =>
          executor.gracefulShutdown(Duration(s, TimeUnit.SECONDS))
          Ok()
        case (Some(s), _) =>
          BadRequest("gracePeriodSeconds should be a positive integer")
        case (None, Some(_)) =>
          executor.hardShutdown()
          Ok()
        case _ =>
          BadRequest(
            "Either gracePeriodSeconds or hard should be specified as query parameter"
          )
      }
    }
  }

  private val queries = Queries(project.logger)

  private[timeseries] def getFocusView(watchedState: WatchedState,
                                       q: CalendarFocusQuery,
                                       filteredJobs: Set[String]): Json = {
    val startDate = Instant.parse(q.start)
    val endDate = Instant.parse(q.end)
    val period = Interval(startDate, endDate)
    val ((jobStates, backfills), _) = watchedState
    val backfillDomain =
      backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
        if (bf.jobs.map(_.id).intersect(filteredJobs).nonEmpty)
          acc.update(Interval(bf.start, bf.end), ())
        else
          acc
      }

    val pausedJobs = scheduler.pausedJobs().map(_.id)
    val allFailingExecutionIds = executor.allFailingExecutions.map(_.id).toSet
    val allWaitingExecutionIds = executor.allRunning
      .filter(_.status == ExecutionWaiting)
      .map(_.id)
      .toSet

    def findAggregationLevel(
      n: Int,
      calendarView: TimeSeriesCalendarView,
      interval: Interval[Instant]
    ): TimeSeriesCalendarView = {
      val aggregatedExecutions = calendarView.calendar.split(interval)
      if (aggregatedExecutions.size <= n)
        calendarView
      else
        findAggregationLevel(n, calendarView.upper(), interval)
    }

    def aggregateExecutions(
      job: TimeSeriesJob,
      period: Interval[Instant],
      calendarView: TimeSeriesCalendarView
    ): List[(Interval[Instant], List[(Interval[Instant], JobState)])] =
      calendarView.calendar
        .split(period)
        .flatMap { interval =>
          {
            val (start, end) = interval
            val currentlyAggregatedPeriod = jobStates(job)
              .intersect(Interval(start, end))
              .toList
              .sortBy(_._1.lo)
            currentlyAggregatedPeriod match {
              case Nil => None
              case _   => Some((Interval(start, end), currentlyAggregatedPeriod))
            }
          }
        }

    def getVersionFromState(jobState: JobState): String = jobState match {
      case Done(version) => version
      case _             => ""
    }

    def getStatusLabelFromState(jobState: JobState, job: Job[TimeSeries]): String =
      jobState match {
        case Todo(_, Some(executionId)) =>
          if (allFailingExecutionIds.contains(executionId))
            "failed"
          else if (allWaitingExecutionIds.contains(executionId))
            "waiting"
          else if (pausedJobs.contains(job.id))
            "paused"
          else "running"
        case Todo(_, _) => if (pausedJobs.contains(job.id)) "paused" else "todo"
        case Done(_)    => "successful"
      }
    val jobTimelines =
      (for { job <- project.jobs.all if filteredJobs.contains(job.id) } yield {
        val calendarView = findAggregationLevel(
          48,
          TimeSeriesCalendarView(job.scheduling.calendar),
          period
        )
        val jobExecutions: List[Option[ExecutionPeriod]] = for {
          (interval, jobStatesOnIntervals) <- aggregateExecutions(
            job,
            period,
            calendarView
          )
        } yield {
          val inBackfill = backfills.exists(
            bf =>
              bf.jobs.contains(job) &&
                IntervalMap(interval -> 0)
                  .intersect(Interval(bf.start, bf.end))
                  .toList
                  .nonEmpty
          )
          if (calendarView.aggregationFactor == 1) {
            jobStatesOnIntervals match {
              case (_, state) :: Nil =>
                Some(
                  JobExecution(
                    interval,
                    getStatusLabelFromState(state, job),
                    inBackfill,
                    getVersionFromState(state)
                  )
                )

              case _ => None
            }
          } else {
            jobStatesOnIntervals match {
              case jobStates: List[(Interval[Instant], JobState)] if jobStates.nonEmpty => {
                val (duration, done, error) =
                  jobStates.foldLeft((0L, 0L, false)) {
                    case (
                        (
                          accumulatedDuration,
                          accumulatedDoneDuration,
                          hasErrors
                        ),
                        (period, jobState)
                        ) =>
                      val (lo, hi) = period.toPair
                      val jobStatus = getStatusLabelFromState(jobState, job)
                      (
                        accumulatedDuration + lo.until(hi, ChronoUnit.SECONDS),
                        accumulatedDoneDuration + (if (jobStatus == "successful")
                                                     lo.until(
                                                       hi,
                                                       ChronoUnit.SECONDS
                                                     )
                                                   else 0),
                        hasErrors || jobStatus == "failed"
                      )
                  }
                Some(
                  AggregatedJobExecution(
                    interval,
                    done.toDouble / duration.toDouble,
                    error,
                    inBackfill
                  )
                )
              }
              case Nil => None
            }
          }
        }
        JobTimeline(job.id, calendarView, jobExecutions.flatten)
      }).toList

    val summary =
      if (jobTimelines.isEmpty) List.empty
      else {
        jobTimelines
          .maxBy(_.executions.size)
          .calendarView
          .calendar
          .split(period)
          .flatMap {
            case (lo, hi) =>
              val isInbackfill =
                backfillDomain.intersect(Interval(lo, hi)).toList.nonEmpty

              case class JobSummary(periodLengthInSeconds: Long, periodDoneInSeconds: Long, hasErrors: Boolean)

              val jobSummaries: Set[JobSummary] = for {
                job <- project.jobs.all
                if filteredJobs.contains(job.id)
                (interval, jobState) <- jobStates(job)
                  .intersect(Interval(lo, hi))
                  .toList
              } yield {
                val (lo, hi) = interval.toPair
                JobSummary(
                  periodLengthInSeconds = lo.until(hi, ChronoUnit.SECONDS),
                  periodDoneInSeconds = jobState match {
                    case Done(_) => lo.until(hi, ChronoUnit.SECONDS)
                    case _       => 0
                  },
                  hasErrors = jobState match {
                    case Todo(_, Some(executionId)) =>
                      allFailingExecutionIds.contains(executionId)
                    case _ => false
                  }
                )
              }
              if (jobSummaries.nonEmpty) {
                val aggregatedJobSummary = jobSummaries.reduce { (a: JobSummary, b: JobSummary) =>
                  JobSummary(
                    a.periodLengthInSeconds + b.periodLengthInSeconds,
                    a.periodDoneInSeconds + b.periodDoneInSeconds,
                    a.hasErrors || b.hasErrors
                  )
                }
                Some(
                  AggregatedJobExecution(
                    Interval(lo, hi),
                    aggregatedJobSummary.periodDoneInSeconds.toDouble / aggregatedJobSummary.periodLengthInSeconds.toDouble,
                    aggregatedJobSummary.hasErrors,
                    isInbackfill
                  )
                )
              } else {
                None
              }
          }

      }

    Json.obj(
      "summary" -> summary.asJson,
      "jobs" -> jobTimelines.map(jt => jt.jobId -> jt).toMap.asJson
    )
  }

  private def snapshotWatchedState() = (scheduler.state, executor.allFailingJobsWithContext)

  private[timeseries] def getFocusView(q: CalendarFocusQuery, jobs: Set[String]): Json =
    getFocusView(snapshotWatchedState(), q, jobs)

  private[timeseries] def publicRoutes(): HttpRoutes[IO] = HttpRoutes.of {
    case request @ GET -> Root / "api" / "timeseries" / "executions" :? JobParam(jobId) :? StartParam(start) :? EndParam(
          end
        ) =>
      def getExecutions(watchedState: WatchedState): IO[Json] = {
        val job = jobs.vertices.find(job => jobId.contains(job.id)).get
        val calendar = job.scheduling.calendar
        val startDate = Instant.parse(start.get)
        val endDate = Instant.parse(end.get)
        val requestedInterval = Interval(startDate, endDate)
        val contextQuery =
          Database.sqlGetContextsBetween(Some(startDate), Some(endDate))
        val archivedExecutions =
          executor.archivedExecutions(
            contextQuery,
            jobId.toSet,
            "",
            asc = true,
            0,
            Int.MaxValue
          )
        val runningExecutions = executor.runningExecutions
          .filter {
            case (e, _) =>
              jobId.contains(e.job.id) && e.context.toInterval.intersects(
                requestedInterval
              )
          }
          .map { case (e, status) => e.toExecutionLog(status) }

        val ((jobStates, _), _) = watchedState
        val remainingExecutions =
          for {
            (interval, maybeBackfill) <- jobStates(job)
              .intersect(requestedInterval)
              .toList
              .collect {
                case (itvl, Todo(maybeBackfill, None)) => (itvl, maybeBackfill)
              }
            (lo, hi) <- calendar.split(interval)
          } yield {
            val context =
              TimeSeriesContext(
                calendar.truncate(lo),
                calendar.ceil(hi),
                maybeBackfill,
                executor.projectVersion
              )
            ExecutionLog(
              "",
              job.id,
              None,
              None,
              context.asJson,
              ExecutionTodo,
              None,
              0
            )
          }
        val throttledExecutions = executor.allFailingExecutions
          .filter(
            e => e.job == job && e.context.toInterval.intersects(requestedInterval)
          )
          .map(_.toExecutionLog(ExecutionThrottled))

        archivedExecutions.map(
          archivedExecutions =>
            ExecutionDetails(
              archivedExecutions ++ runningExecutions ++ remainingExecutions ++ throttledExecutions,
              parentExecutions(requestedInterval, job, jobStates)
            ).asJson
        )
      }

      def parentExecutions(
        requestedInterval: Interval[Instant],
        job: Job[TimeSeries],
        state: Map[Job[TimeSeries], IntervalMap[Instant, JobState]]
      ): Seq[ExecutionLog] = {

        val calendar = job.scheduling.calendar
        val parentJobs = jobs.edges
          .collect({ case (child, parent, _) if child == job => parent })
        val runningDependencies: Seq[ExecutionLog] = executor.runningExecutions
          .filter {
            case (e, _) =>
              parentJobs.contains(e.job) && e.context.toInterval.intersects(
                requestedInterval
              )
          }
          .map({ case (e, status) => e.toExecutionLog(status) })
        val failingDependencies: Seq[ExecutionLog] =
          executor.allFailingExecutions
            .filter(
              e =>
                parentJobs.contains(e.job) && e.context.toInterval
                  .intersects(requestedInterval)
            )
            .map(_.toExecutionLog(ExecutionThrottled))
        val remainingDependenciesDeps =
          for {
            parentJob <- parentJobs
            (interval, maybeBackfill) <- state(parentJob)
              .intersect(requestedInterval)
              .toList
              .collect {
                case (itvl, Todo(maybeBackfill, _)) => (itvl, maybeBackfill)
              }
            (lo, hi) <- calendar.split(interval)
          } yield {
            val context =
              TimeSeriesContext(
                calendar.truncate(lo),
                calendar.ceil(hi),
                maybeBackfill,
                executor.projectVersion
              )
            ExecutionLog(
              "",
              parentJob.id,
              None,
              None,
              context.asJson,
              ExecutionTodo,
              None,
              0
            )
          }

        runningDependencies ++ failingDependencies ++ remainingDependenciesDeps.toSeq
      }

      if (reqSupportsEventStream(request)) {
        sse(
          IO { Some(snapshotWatchedState()) },
          (s: WatchedState) => getExecutions(s)
        )
      } else {
        getExecutions(snapshotWatchedState()).flatMap(Ok(_))
      }

    case GET -> Root / "api" / "timeseries" / "calendar" / "focus" :? JobsParam(jobs) :? StartParam(start) :? EndParam(
          end
        ) =>
      val filteredJobs = Option(jobs.toIterable.flatMap(_.split(",")).filterNot(_.isEmpty).toSet)
        .filterNot(_.isEmpty)
        .getOrElse(allIds)
      val q = CalendarFocusQuery(filteredJobs, start.get, end.get)

      Ok(getFocusView(q, filteredJobs))

    case request @ POST -> Root / "api" / "timeseries" / "calendar" / "focus" =>
      request
        .as[Json]
        .flatMap { json =>
          json
            .as[CalendarFocusQuery]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              query => {
                val jobs = Option(query.jobs.filterNot(_.isEmpty))
                  .filterNot(_.isEmpty)
                  .getOrElse(allIds)
                Ok(getFocusView(query, jobs))
              }
            )
        }

    case request @ POST -> Root / "api" / "timeseries" / "calendar" => {

      case class JobStateOnPeriod(start: Instant, duration: Long, isDone: Boolean, isStuck: Boolean)

      def getCalendar(watchedState: WatchedState, jobs: Set[String]): Json = {
        val ((jobStates, backfills), _) = watchedState
        val backfillDomain =
          backfills.foldLeft(IntervalMap.empty[Instant, Unit]) { (acc, bf) =>
            if (bf.jobs.map(_.id).intersect(jobs).nonEmpty)
              acc.update(Interval(bf.start, bf.end), ())
            else
              acc
          }
        val upToMidnightToday =
          Interval(Bottom, Finite(Daily(UTC).ceil(Instant.now)))

        lazy val failingExecutionIds =
          executor.allFailingExecutions.map(_.id).toSet
        val jobStatesOnPeriod: Set[JobStateOnPeriod] = for {
          job <- project.jobs.all
          if jobs.contains(job.id)
          (interval, jobState) <- jobStates(job)
            .intersect(upToMidnightToday)
            .toList
          (start, end) <- Daily(UTC).split(interval)
        } yield JobStateOnPeriod(
          Daily(UTC).truncate(start),
          start.until(end, ChronoUnit.SECONDS),
          jobState match {
            case Done(_) => true
            case _       => false
          },
          jobState match {
            case Todo(_, Some(exec)) => failingExecutionIds.contains(exec)
            case _                   => false
          }
        )

        jobStatesOnPeriod
          .groupBy { case JobStateOnPeriod(start, _, _, _) => start }
          .toList
          .sortBy { case (periodStart, _) => periodStart }
          .map {
            case (date, statesOnPeriod) =>
              val (total, done, stuck) =
                statesOnPeriod.foldLeft((0L, 0L, false)) {
                  case (acc, JobStateOnPeriod(_, duration, isDone, isStuck)) =>
                    val (totalDuration, doneDuration, isAnyStuck) = acc
                    val newDone = if (isDone) duration else 0L
                    (
                      totalDuration + duration,
                      doneDuration + newDone,
                      isAnyStuck || isStuck
                    )
                }
              val completion = Math.floor((done.toDouble / total.toDouble) * 10) / 10
              val correctedCompletion =
                if (completion == 0 && done != 0) 0.1
                else completion
              Map(
                "date" -> date.atZone(UTC).toLocalDate.asJson,
                "completion" -> correctedCompletion.asJson
              ) ++ (if (stuck) Map("stuck" -> true.asJson) else Map.empty) ++
                (if (backfillDomain
                       .intersect(Interval(date, Daily(UTC).next(date)))
                       .toList
                       .nonEmpty)
                   Map("backfill" -> true.asJson)
                 else Map.empty)
          }
          .asJson
      }

      request
        .as[Json]
        .flatMap { json =>
          json.hcursor
            .downField("jobs")
            .as[Set[String]]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              jobIds => {
                val ids =
                  if (jobIds.isEmpty) project.jobs.all.map(_.id) else jobIds
                Ok(getCalendar(snapshotWatchedState(), ids))
              }
            )
        }
    }

    case GET -> Root / "api" / "timeseries" / "lastruns" :? JobParam(jobId) =>
      val (jobStates, _) = scheduler.state
      val successfulIntervalMaps = jobStates
        .filter(s => jobId.contains(s._1.id))
        .values
        .flatMap(m => m.toList)
        .filter {
          case (interval, jobState) =>
            jobState match {
              case Done(_) => true
              case _       => false
            }
        }
        .foldLeft(IntervalMap.empty[Instant, Unit])(
          (acc, elt) => acc.update(elt._1, ())
        )
        .toList

      if (successfulIntervalMaps.isEmpty) NotFound()
      else {
        (successfulIntervalMaps.head._1.hi, successfulIntervalMaps.last._1.hi) match {
          case (Finite(lastCompleteTime), Finite(lastTime)) =>
            Ok(
              Json.obj(
                "lastCompleteTime" -> lastCompleteTime.asJson,
                "lastTime" -> lastTime.asJson
              )
            )
          case _ => BadRequest()
        }
      }

    case GET -> Root / "api" / "timeseries" / "backfills" =>
      Database
        .queryBackfills()
        .to[List]
        .map(_.map {
          case (
              id,
              name,
              description,
              jobs,
              priority,
              start,
              end,
              created_at,
              status,
              created_by
              ) =>
            Json.obj(
              "id" -> id.asJson,
              "name" -> name.asJson,
              "description" -> description.asJson,
              "jobs" -> jobs.asJson,
              "priority" -> priority.asJson,
              "start" -> start.asJson,
              "end" -> end.asJson,
              "created_at" -> created_at.asJson,
              "status" -> status.asJson,
              "created_by" -> created_by.asJson
            )
        })
        .transact(xa)
        .flatMap(backfills => Ok(backfills.asJson))
    case GET -> Root / "api" / "timeseries" / "backfills" / id :? EventsParam(events) =>
      val backfills = Database.getBackfillById(id).transact(xa)
      events match {
        case Some("true" | "yes") => sse(backfills, (b: Json) => IO.pure(b))
        case _                    => backfills.flatMap(bf => Ok(bf.asJson))
      }
    case request @ POST -> Root / "api" / "timeseries" / "backfills" / backfillId / "executions" => {
      def allExecutions(
        q: ExecutionsQuery
      ): IO[Option[(Int, Double, List[ExecutionLog])]] = {

        val ordering = {
          val columnOrdering = q.sort.column match {
            case "job"       => Ordering.by((_: ExecutionLog).job)
            case "startTime" => Ordering.by((_: ExecutionLog).startTime)
            case "status"    => Ordering.by((_: ExecutionLog).status.toString)
            case _           => Ordering.by((_: ExecutionLog).id)
          }
          if (q.sort.asc) {
            columnOrdering
          } else {
            columnOrdering.reverse
          }
        }

        val runningExecutions = executor.runningExecutions
          .filter(t => {
            val bf = t._1.context.backfill
            bf.isDefined && bf.get.id == backfillId
          })
          .map({ case (execution, status) => execution.toExecutionLog(status) })

        val runningExecutionsIds = runningExecutions.map(_.id).toSet
        Database
          .getExecutionLogsForBackfill(backfillId)
          .transact(xa)
          .map(archived => {
            val archivedNotRunning =
              archived.filterNot(e => runningExecutionsIds.contains(e.id))
            val executions = runningExecutions ++ archivedNotRunning
            val completion = {
              executions.size match {
                case 0     => 0
                case total => (total - runningExecutions.size).toDouble / total
              }
            }
            Some(
              (
                executions.size,
                completion,
                executions.sorted(ordering).drop(q.offset).take(q.limit).toList
              )
            )
          })
      }

      request
        .as[Json]
        .flatMap { json =>
          json
            .as[ExecutionsQuery]
            .fold(
              df => BadRequest(s"Error: Cannot parse request body: $df"),
              q => {
                allExecutions(q)
                  .flatMap(_.map {
                    case (total, completion, executions) =>
                      Ok(
                        Json.obj(
                          "total" -> total.asJson,
                          "offset" -> q.offset.asJson,
                          "limit" -> q.limit.asJson,
                          "sort" -> q.sort.column.asJson,
                          "asc" -> q.sort.asc.asJson,
                          "data" -> executions.asJson,
                          "completion" -> completion.asJson
                        )
                      )
                  }.getOrElse(NotFound()))
              }
            )
        }
    }
  }

  private[timeseries] def privateRoutes(): Auth.Routes = Auth.Routes.of {
    case req @ POST -> Root / "api" / "timeseries" / "backfill" => { implicit user =>
      req
        .as[Json]
        .flatMap(
          _.as[BackfillCreate]
            .fold(
              df => BadRequest(s"""
                         |Error during backfill creation.
                         |Error: Cannot parse request body.
                         |$df
                         |""".stripMargin),
              backfill => {
                val jobIdsToBackfill = backfill.jobs.toSet
                jobIdsToBackfill
                  .partition(j => jobs.all.map(_.id).contains(j)) match {
                  case (_, r) if !r.isEmpty =>
                    BadRequest(
                      s"Contains unknown job ids: ${r.map(s => s"'$s'").mkString(",")}"
                    )
                  case (filtered, _) =>
                    scheduler
                      .backfillJob(
                        backfill.name,
                        backfill.description,
                        jobs.all.filter(j => filtered.contains(j.id)),
                        backfill.startDate,
                        backfill.endDate,
                        backfill.priority,
                        executor.runningState,
                        xa
                      )
                      .flatMap {
                        case Right(_)     => Ok("ok".asJson)
                        case Left(errors) => BadRequest(errors)
                      }
                }
              }
            )
        )
    }

    // consider the given period of the job as successful, regardless of it's actual status
    case GET -> Root / "api" / "timeseries" / "force-success" :? JobParam(jobId) :? StartParam(start) :? EndParam(
          end
        ) => { implicit user =>
      (for {
        startDate <- Try(Instant.parse(start.get))
        endDate <- Try(Instant.parse(end.get))
        job <- Try(
          project.jobs.all
            .find(jobId.contains)
            .getOrElse(throw new Exception(s"Unknow job $jobId"))
        )
      } yield {
        val requestedInterval = Interval(startDate, endDate)
        scheduler.forceSuccess(
          job,
          requestedInterval,
          executor.projectVersion
        )
        def filterOp(e: Execution[TimeSeries]): Boolean = {
          val contextInterval = Interval(e.context.start, e.context.end)
          jobId.contains(e.job.id) && contextInterval.intersects(requestedInterval)
        }
        val runningExecutions = executor.runningExecutions.collect {
          case (e, s) if filterOp(e) => e
        }
        val failingExecutions = executor.allFailingExecutions.filter(filterOp)
        val executions = runningExecutions ++ failingExecutions
        executions.foreach(_.cancel())
        (
          executions.length,
          JobSuccessForced(Instant.now(), user, jobId.get, startDate, endDate)
        )
      }) match {
        case Success((canceledExecutions, event)) =>
          queries
            .logEvent(event)
            .transact(xa)
            .flatMap(
              _ =>
                Ok(
                  Json.obj(
                    "canceled-executions" -> Json.fromInt(canceledExecutions)
                  )
                )
            )
        case Failure(e) =>
          BadRequest(Json.obj("error" -> Json.fromString(e.getMessage)))
      }
    }
  }

  val api = publicApi orElse project.authenticator(privateApi)

  val publicAssets: HttpRoutes[IO] = HttpRoutes.of {
    case GET -> Root / "public" / file =>
      import Implicits.serverContextShift
      StaticFile.fromResource[IO](s"/public/$file", Implicits.serverThreadPool).getOrElseF(NotFound())
  }

  val index: Auth.Routes = Auth.Routes.of {
    case _ -> Root / "api" / _ =>
      _ => NotFound()
    case _ =>
      import Implicits.serverContextShift
      _ => StaticFile.fromResource[IO]("/public/index.html", Implicits.serverThreadPool).getOrElseF(NotFound())
  }

  /** List of */
  val routes: HttpApp[IO] = (
    api <+>
      publicRoutes() <+>
      project.authenticator(privateRoutes()) <+> {
      executor.platforms.foldLeft(HttpRoutes.empty[IO]) {
        case (s, p) =>
          s.orElse(p.publicRoutes)
            .orElse(project.authenticator(p.privateRoutes))
      }
    } <+>
      publicAssets <+>
      project.authenticator(index)
  ).orNotFound
}
