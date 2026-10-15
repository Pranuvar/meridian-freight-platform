"""Export Meridian's landing zone as ZIP files for upload to a Databricks Volume."""
import json, re, shutil, zipfile
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.utils.config import BRONZE, DATA, RAW
from src.utils.spark import get_spark

OUT = DATA / "landing_export"
STAGE = OUT / "_stage"
CUTOFF_DAY = "20260531"     # scans_YYYYMMDD files after this day belong to the increment
CUTOFF_MONTH = "2026-05"    # invoices_*_YYYY-MM.csv files after this month belong to the increment
LINEAGE = ["_ingested_at", "_source_system", "_source_object", "_batch_id", "_processing_date"]
ZIPS = ("initial", "increment")


def part_of(is_initial):
    return "initial" if is_initial else "increment"


def stage_dir(part, *sub):
    p = STAGE / part
    for s in sub:
        p = p / s
    p.mkdir(parents=True, exist_ok=True)
    return p


def read_bronze(spark, path):
    return spark.read.format("delta").load(str(path))


def export_tms(spark):
    """One Parquet file per (table, Bronze batch). Lineage columns dropped: Databricks adds its own."""
    for d in sorted(BRONZE.glob("tms_*")):
        if not DeltaTable.isDeltaTable(spark, str(d)):
            continue
        table = d.name[len("tms_"):]
        df = read_bronze(spark, d)
        batches = [r[0] for r in df.select("_batch_id").distinct().orderBy("_batch_id").collect()]
        for b in batches:
            dest = stage_dir(part_of(b.startswith("initial")), "tms", table)
            tmp = dest / f"_tmp_{b}"
            # coalesce(1) gives exactly one part file, which we rename to a meaningful name.
            (df.filter(F.col("_batch_id") == b).drop(*LINEAGE).coalesce(1)
               .write.mode("overwrite").parquet(str(tmp)))
            next(tmp.glob("part-*.parquet")).rename(dest / f"{table}__{b}.parquet")
            shutil.rmtree(tmp)
            print(f"  tms/{table:<18} batch {b}")


def copy_files():
    for p in sorted((RAW / "scans").glob("scans_*.parquet")):
        day = re.search(r"scans_(\d{8})", p.name).group(1)
        shutil.copy2(p, stage_dir(part_of(day <= CUTOFF_DAY), "scans") / p.name)
    for p in sorted((RAW / "invoices").glob("*/invoices_*.csv")):
        month = re.search(r"_(\d{4}-\d{2})\.csv$", p.name).group(1)
        shutil.copy2(p, stage_dir(part_of(month <= CUTOFF_MONTH), "invoices", p.parent.name) / p.name)
    for p in sorted((RAW / "api").glob("*/*.json")):
        shutil.copy2(p, stage_dir(part_of("_initial_" in p.name), "api", p.parent.name) / p.name)


def count_rows(spark, part):
    """Row counts per Bronze table, read back from the STAGED files: the manifest describes what is in the zip."""
    root, rows = STAGE / part, {}
    for d in sorted((root / "tms").glob("*")) if (root / "tms").exists() else []:
        rows[f"tms_{d.name}"] = spark.read.parquet(str(d)).count()
    if (root / "scans").exists():
        rows["scans"] = spark.read.option("mergeSchema", "true").parquet(str(root / "scans")).count()
    for d in sorted((root / "invoices").glob("*")) if (root / "invoices").exists() else []:
        rows[f"invoices_{d.name.lower()}"] = spark.read.option("header", "true").csv(str(d)).count()
    for d in sorted((root / "api").glob("*")) if (root / "api").exists() else []:
        rows[f"api_{d.name}"] = spark.read.option("multiLine", "true").json(str(d)).count()
    return rows


def make_zip(spark, part):
    root = STAGE / part
    if not root.exists():
        print(f"{part}: nothing to export")
        return
    manifest = {"part": part, "rows": count_rows(spark, part)}
    (root / f"manifest_{part}.json").write_text(json.dumps(manifest, indent=2))
    target = OUT / f"meridian_landing_{part}.zip"
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(root.rglob("*")):
            if f.is_file():
                z.write(f, f.relative_to(root).as_posix())
    print(f"\n{target}  ({target.stat().st_size / 1e6:.1f} MB)")
    for k, v in manifest["rows"].items():
        print(f"  {k:<22}{v:>12,}")


def main():
    if STAGE.exists():
        shutil.rmtree(STAGE)          # always rebuild from scratch: the export is reproducible
    OUT.mkdir(parents=True, exist_ok=True)
    spark = get_spark("landing_export")
    # TIMESTAMP_MICROS keeps timestamps as UTC instants, so Databricks reads TIMESTAMP, not TIMESTAMP_NTZ.
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    export_tms(spark)
    copy_files()
    for part in ZIPS:
        make_zip(spark, part)
    shutil.rmtree(STAGE)
    spark.stop()


if __name__ == "__main__":
    main()
