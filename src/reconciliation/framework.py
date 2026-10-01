"""Reconciliation framework: collect every check, print ONE report, persist the results.

Four outcomes:
  PASS       two numbers that must agree do agree (within a stated tolerance)
  EXPLAINED  a gap that is EXPECTED (e.g. rejected records) and is fully classified: nothing left over
  INFO       a number worth seeing, no pass/fail
  FAIL       something does not add up: a reconciliation BREAK
For EXPLAINED rows: left = the gap, right = the part that was classified, difference = what is left UNEXPLAINED.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from src.utils.config import CONTROL


@dataclass
class Result:
    check: str
    from_layer: str
    to_layer: str
    left: Optional[Decimal]
    right: Optional[Decimal]
    difference: Optional[Decimal]
    status: str
    note: str = ""


def _num(x):
    # Go through str() so a float such as 0.1 becomes Decimal('0.1'), not its long binary expansion.
    return None if x is None else Decimal(str(x))


class Recon:
    def __init__(self):
        self.results = []

    def equal(self, check, f, t, expected, actual, tol=0, note=""):
        """Two numbers that must agree. tol is an ABSOLUTE tolerance (0 = exact)."""
        e, a = _num(expected), _num(actual)
        d = a - e
        ok = abs(d) <= Decimal(str(tol))
        self.results.append(Result(check, f, t, e, a, d, "PASS" if ok else "FAIL", note))
        return ok

    def explained(self, check, f, t, gap, explained, tol=0, note=""):
        """A gap that is allowed to exist but must be completely classified."""
        g, x = _num(gap), _num(explained)
        left = g - x
        ok = abs(left) <= Decimal(str(tol))
        self.results.append(Result(check, f, t, g, x, left, "EXPLAINED" if ok else "FAIL", note))
        return ok

    def band(self, check, f, t, value, lo, hi, note=""):
        """Statistical reasonableness: the value must sit inside a historical range."""
        v = _num(value)
        ok = Decimal(str(lo)) <= v <= Decimal(str(hi))
        self.results.append(Result(check, f, t, v, None, None, "PASS" if ok else "FAIL",
                                   f"expected between {lo} and {hi}. {note}"))
        return ok

    def info(self, check, f, t, value, note=""):
        self.results.append(Result(check, f, t, _num(value), None, None, "INFO", note))

    @property
    def failed(self):
        return any(r.status == "FAIL" for r in self.results)

    def report(self):
        def fmt(v):
            if v is None:
                return ""
            return f"{int(v):,}" if v == v.to_integral_value() else f"{v.quantize(Decimal('0.01')):,}"
        w = max(len(r.check) for r in self.results)
        print(f"\n{'check':<{w}}  {'flow':<36}{'left':>20}{'right':>20}{'difference':>16}  status")
        print("-" * (w + 100))
        for r in self.results:
            flow = f"{r.from_layer} -> {r.to_layer}"
            print(f"{r.check:<{w}}  {flow:<36}{fmt(r.left):>20}{fmt(r.right):>20}{fmt(r.difference):>16}  {r.status}")
            if r.note:
                print(f"{'':<{w}}    {r.note}")
        counts = {s: sum(1 for r in self.results if r.status == s) for s in ("PASS", "EXPLAINED", "INFO", "FAIL")}
        print("\n" + "   ".join(f"{s}: {c}" for s, c in counts.items()))
        print("RECONCILIATION: " + ("BREAKS FOUND" if self.failed else "CLEAN (every gap is zero or fully explained)"))

    def save(self, spark):
        """Append this run to a control table so the history is queryable (and chartable in Power BI)."""
        ts = datetime.now(timezone.utc)
        s = lambda v: None if v is None else str(v)      # noqa: E731
        rows = [(ts, r.check, r.from_layer, r.to_layer, s(r.left), s(r.right), s(r.difference), r.status, r.note)
                for r in self.results]
        schema = ("run_ts timestamp, check_name string, from_layer string, to_layer string, left_value string, "
                  "right_value string, difference string, status string, note string")
        spark.createDataFrame(rows, schema).write.format("delta").mode("append").save(str(CONTROL / "recon_results"))
