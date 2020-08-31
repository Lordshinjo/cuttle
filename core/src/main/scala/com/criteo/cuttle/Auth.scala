package com.criteo.cuttle

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.http4s.server.AuthMiddleware
import org.http4s.{AuthedRequest, AuthedRoutes, Request, Response}

/**
  * The cuttle API is private for any write operation while it is publicly
  * open for any read only operation. It allows to make it easy to build tooling
  * that monitor any running cuttle scheduler while restricting access to potentially
  * dangerous operations.
  *
  * The UI access itself requires authentication.
  */
object Auth {

  /**
    * A connected [[User]].
    */
  case class User(userId: String)

  object User {
    implicit val encoder: Encoder[User] = deriveEncoder
    implicit val decoder: Decoder[User] = deriveDecoder
  }

  type Routes = AuthedRoutes[User, IO]

  object Routes {
    def of(pf: PartialFunction[Request[IO], Function[User, IO[Response[IO]]]]): Routes = AuthedRoutes {
      case AuthedRequest(user, req)=>
        pf.lift(req) match {
          case Some(userFn) => OptionT.liftF(userFn(user))
          case None => OptionT.none
        }
    }
  }

  type Authenticator = AuthMiddleware[IO, User]

  val GuestAuth: Authenticator = AuthMiddleware.withFallThrough[IO, User](Kleisli { _ =>
    OptionT.some(User("Guest"))
  })
}
