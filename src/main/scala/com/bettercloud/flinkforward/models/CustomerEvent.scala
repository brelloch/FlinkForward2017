package com.bettercloud.flinkforward.models

import java.util.UUID

case class CustomerEvent(customerId: UUID, payload: String)

object CustomerEvent extends ObjectMapperTrait[CustomerEvent] {
  def apply(s:String): CustomerEvent = {
    CustomerEvent.fromJson(s)
  }
}