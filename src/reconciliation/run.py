"""Run every reconciliation check, print one report, save the results. Exit code 1 if anything FAILS.
Usage: python -m src.reconciliation.run"""
import sys

from src.reconciliation import checks_business as biz
from src.reconciliation import checks_flow as flow
from src.reconciliation.framework import Recon
from src.utils.spark import get_spark


def main():
    spark = get_spark("reconciliation")
    rc = Recon()
    flow.entity_flow(spark, rc)
    flow.charge_amounts(spark, rc)
    flow.scans_flow(spark, rc)
    flow.invoices_flow(spark, rc)
    biz.layer_counts(spark, rc)
    biz.revenue(spark, rc)
    biz.fx_variance(spark, rc)
    biz.carrier_cost(spark, rc)
    biz.orphans(spark, rc)
    biz.delivered_gap(spark, rc)
    rc.report()
    rc.save(spark)
    spark.stop()
    if rc.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
