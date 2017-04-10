package com.bettercloud.flinkforward.models

case class FilteredEvent(event: CustomerEvent, controls: List[ControlEvent])

