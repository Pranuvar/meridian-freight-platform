from src.reconciliation.framework import Recon


def test_equal_respects_the_tolerance():
    rc = Recon()
    assert rc.equal("exact", "a", "b", 100, 100)
    assert rc.equal("within tolerance", "a", "b", 100.004, 100.0, tol=0.01)
    assert not rc.equal("off by one", "a", "b", 100, 101)
    assert [r.status for r in rc.results] == ["PASS", "PASS", "FAIL"]
    assert rc.failed


def test_a_gap_is_explained_only_when_nothing_is_left_over():
    rc = Recon()
    rc.explained("fully classified", "a", "b", 630, 630)
    rc.explained("partly classified", "a", "b", 630, 600)
    assert [r.status for r in rc.results] == ["EXPLAINED", "FAIL"]
    assert rc.results[1].difference == 30          # exactly how much is still unexplained
