"""Health decisions as small pure functions (no Spark), so they are trivial to test."""
import statistics


def freshness(hours_behind: float, limit_hours: float) -> str:
    return "OK" if hours_behind <= limit_hours else "STALE"


def volume_anomaly(history, latest, low=0.5, high=2.0, min_history=3):
    """Compare the latest run's row count with the MEDIAN of earlier runs. The median ignores one odd past run, which a
    mean would not. With too little history we say so, instead of guessing."""
    if len(history) < min_history:
        return "INSUFFICIENT_HISTORY", f"{len(history)} earlier run(s), need {min_history}"
    med = statistics.median(history)
    if med == 0:
        return ("OK" if latest == 0 else "ANOMALY"), "history median is 0"
    ratio = latest / med
    return ("OK" if low <= ratio <= high else "ANOMALY"), f"latest {latest:,} vs median {med:,.0f} ({ratio:.2f}x)"
