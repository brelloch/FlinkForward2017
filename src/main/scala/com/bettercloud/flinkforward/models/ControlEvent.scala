package com.bettercloud.flinkforward.models

import java.util.UUID

case class ControlEvent(customerId: UUID, alertId: UUID, alertName: String, alertDescription: String, threshold: Int, jsonPath: String, bootstrapCustomerId: UUID)

object ControlEvent extends ObjectMapperTrait[ControlEvent]