package org.apache.spark.sql.execution.adaptive
/** Test-only observer log-capacity fault injection; unrelated to API budgets. */
object ObserverFaultInjector {
  def saturateAuditCounterForTest():Unit = { NativeBoundaryObserverV6.pairs.set(1000L) }
}
