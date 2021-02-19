package com.vyunsergey.filesystemwatcher.common.status

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

sealed trait ProcessingStatus

object ProcessingStatus {
  case class Success(status: String = "SUCCESS", message: String) extends ProcessingStatus
  case class Failure(status: String = "ERROR", message: String) extends ProcessingStatus

  implicit val processingStatusDecoder: Decoder[ProcessingStatus] = deriveDecoder[ProcessingStatus]
  implicit val processingStatusEncoder: Encoder[ProcessingStatus] = deriveEncoder[ProcessingStatus]

  implicit val processingStatusSuccessDecoder: Decoder[Success] = deriveDecoder[Success]
  implicit val processingStatusSuccessEncoder: Encoder[Success] = deriveEncoder[Success]

  implicit val processingStatusFailureDecoder: Decoder[Failure] = deriveDecoder[Failure]
  implicit val processingStatusFailureEncoder: Encoder[Failure] = deriveEncoder[Failure]
}
