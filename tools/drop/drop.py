"""Course gate CLI.

    drop next [--force]   gate on the current drop, then copy the next one into your landing bucket
    drop check silver     run reference/silver-contract.odcs.yaml against your silver table

Install (devcontainer, once): uv tool install --editable tools/drop   -> `drop` on PATH.
Editable on purpose: the contract is read from this repo's reference/ folder.

Config (env):
    DROP_HANDLE   your handle; default: taken from the cwd when you are inside learners/<handle>/
    DROP_PREFIX   bucket prefix; default gs://<project>- (project from GOOGLE_CLOUD_PROJECT or gcloud config).
                  Tests point it at a local dir: /tmp/x/p- gives /tmp/x/p-drops, /tmp/x/p-<handle>-landing, ...

State: the current drop is the highest dropN/ folder in your landing bucket. Nothing else is stored,
so undoing a forced drop is just `gcloud storage rm -r gs://kuikops-lakehouse-<handle>-landing/dropN/`.
Log: every delivery and refusal writes one object to <prefix>drops/log/<handle>/ (one object per
event, never overwritten). The instructor reads it with `gcloud storage ls -r gs://kuikops-lakehouse-drops/log/`.
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

LAST_DROP = 5
COLS = ["code", "product_name", "brands", "categories", "countries",
        "nutrition_grade_fr", "sugars_100g", "last_modified"]
CONTRACT = Path(__file__).resolve().parents[2] / "reference" / "silver-contract.odcs.yaml"
GCS_CONNECTOR = ("https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/"
                 "3.1.19/gcs-connector-3.1.19-shaded.jar")


class Refused(Exception):
    pass


# where a learner rebuilds each table: appended to gate messages so a refusal says where to look
LESSON = {"bronze": "lesson 3, section 3", "silver": "lesson 5, section 5",
          "quarantine": "lesson 4, section 4", "gold": "lesson 6, section 3"}


# ---------- config ----------

def handle():
    h = os.environ.get("DROP_HANDLE")
    if not h:
        parts = Path.cwd().parts
        if "learners" in parts and parts.index("learners") + 1 < len(parts):
            h = parts[parts.index("learners") + 1]
    if not h or not re.fullmatch(r"[a-z][a-z0-9]{1,19}", h):
        raise Refused(f"no valid handle (got {h!r}, a handle is 2 to 20 lowercase letters or digits and starts "
                      f"with a letter): run drop from learners/<handle>/ or set DROP_HANDLE")
    return h


def prefix():
    p = os.environ.get("DROP_PREFIX")
    if p:
        return p
    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or subprocess.run(
        ["gcloud", "config", "get-value", "project"], capture_output=True, text=True).stdout.strip()
    if not project:
        raise Refused("no GCP project: run `gcloud config set project kuikops-lakehouse` (lesson 0)")
    return f"gs://{project}-"


def paths(h, p):
    return {
        "drops": f"{p}drops",
        "landing": f"{p}{h}-landing",
        "bronze": f"{p}{h}-bronze/products",
        "silver": f"{p}{h}-silver/products",
        "quarantine": f"{p}{h}-silver/quarantine",
        "gold": f"{p}{h}-gold",
    }


def spark_session(gcs):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.master("local[2]").appName("drop")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.ui.showConsoleProgress", "false"))
    if gcs:  # a laptop Spark has no gs:// filesystem: add the connector, auth with your gcloud ADC
        b = (b.config("spark.jars", GCS_CONNECTOR)
             .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
             .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
             .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT"))
    spark = configure_spark_with_delta_pip(b).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------- storage (Hadoop FS, so gs:// and local dirs take the same code path) ----------

def _fs(spark, path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def exists(spark, path):
    fs, p = _fs(spark, path)
    return fs.exists(p)


def current_drop(spark, landing):
    return max([n for n in range(1, LAST_DROP + 1) if exists(spark, f"{landing}/drop{n}")], default=0)


def deliver(spark, P, n):
    src_fs, src = _fs(spark, f"{P['drops']}/drop{n}")
    dst_fs, dst = _fs(spark, f"{P['landing']}/drop{n}")
    spark._jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, src, dst_fs, dst, False,
                                                  spark._jsc.hadoopConfiguration())


def log(spark, P, h, n, event, detail=None):
    now = datetime.datetime.now(datetime.timezone.utc)
    body = json.dumps({"handle": h, "drop": n, "event": event, "at": now.isoformat(),
                       "user": os.environ.get("USER"), "detail": detail})
    fs, p = _fs(spark, f"{P['drops']}/log/{h}/{now:%Y%m%dT%H%M%S%fZ}-drop{n}-{event}.json")
    out = fs.create(p, False)
    out.write(bytearray(body.encode()))
    out.close()


# ---------- gates: each returns the list of what it could not find (empty = pass) ----------

def _table(spark, path, what):
    from delta import DeltaTable
    if not DeltaTable.isDeltaTable(spark, path):
        raise Refused(f"no Delta table at {path} ({what}): {LESSON[what]}")
    return spark.read.format("delta").load(path)


def _drop(spark, P, n):
    return spark.read.option("header", True).csv(f"{P['drops']}/drop{n}")


def _missing_cols(df, cols, path):
    return [f"{path} has no column {c}" for c in cols if c not in df.columns]


def _stamped_bronze(spark, P, extra=()):
    from pyspark.sql import functions as F
    b = _table(spark, P["bronze"], "bronze")
    gaps = _missing_cols(b, COLS + ["source_file", "loaded_at", *extra], P["bronze"])
    if gaps:
        raise Refused("; ".join(gaps) + (" (add the column on purpose with ALTER TABLE ... ADD COLUMNS: lesson 7, section 4)" if extra
                                         else " (stamp each row in your bronze hop with _metadata.file_path as source_file "
                                              "and current_timestamp() as loaded_at: lesson 2, section 2)"))
    return b.filter(F.col("source_file").isNotNull() & F.col("loaded_at").isNotNull())


def _first_code(df):
    row = df.select("code").orderBy("code").first()
    return row["code"] if row else None


def gate_2(spark, P):
    d1 = _drop(spark, P, 1).select(COLS)
    missing = d1.subtract(_stamped_bronze(spark, P).select(COLS))
    n = missing.count()
    return [(f"{n} of {d1.count()} drop-1 rows are not in {P['bronze']} with source_file and loaded_at "
             f"(first missing code: {_first_code(missing)}). Check that your bronze batch appends every file "
             f"under landing/drop1/ and fills both stamps: {LESSON['bronze']}.")] if n else []


def gate_3(spark, P):
    from pyspark.sql import functions as F
    out = []
    silver = _table(spark, P["silver"], "silver")
    dups = silver.groupBy("code").count().filter("count > 1")
    if (n := dups.count()):
        out.append(f"{P['silver']} has {n} codes on more than one row (first: {_first_code(dups)}). Keep only the latest "
                   f"last_modified per code before the MERGE, then MERGE into silver on the business key code: "
                   f"lesson 5, sections 3 and 4.")
    q = _table(spark, P["quarantine"], "quarantine")
    # lesson 4's stretch replaces the rule column with a rules array: accept both
    if "rules" in q.columns:
        is_missing = F.array_contains("rules", "code_missing")
    elif "rule" in q.columns:
        is_missing = F.col("rule") == "code_missing"
    else:
        return out + [f"{P['quarantine']} has no column rule (lesson 4, section 2)"]
    key = ["product_name", "brands", "last_modified"]
    empty = _drop(spark, P, 2).filter(F.col("code").isNull() | (F.trim("code") == "")).select(key)
    n = empty.subtract(q.filter(is_missing).select(key)).count()
    if n:
        out.append(f"{n} of {empty.count()} drop-2 rows with an empty code are not in "
                   f"{P['quarantine']} under rule code_missing. Check that your split sends a null or blank code "
                   f"(trim(code) = '') to quarantine before any other rule: {LESSON['quarantine']}.")
    return out


def gate_4(spark, P):
    from pyspark.sql import functions as F
    out = []
    silver = _table(spark, P["silver"], "silver")
    sugar = F.col("sugars_100g")
    parsed_sugar = F.expr(r"try_cast(regexp_replace(trim(sugars_100g), '\\s*g$', '') AS double)")
    date = F.coalesce(F.to_date(F.try_to_timestamp("last_modified", F.lit("yyyy-MM-dd"))),
                      F.to_date(F.try_to_timestamp("last_modified", F.lit("dd/MM/yyyy"))))
    rows = (_drop(spark, P, 3)
            .filter(F.trim("code") != "")
            .filter(sugar.rlike(r"\d\s*g$") | F.col("last_modified").rlike(r"^\d{2}/\d{2}/\d{4}$"))
            .filter(sugar.isNull() | parsed_sugar.isNotNull())  # "lots" is not salvageable: skip it
            .select("code", date.alias("d")))
    # in silver = that code with this date or a later one (a late update loses to the newer row)
    s = silver.select("code", F.col("last_modified").alias("s"))
    missing = rows.join(s, (rows.code == s.code) & (F.col("s") >= F.col("d")), "left_anti")
    if (n := missing.count()):
        out.append(f"{n} of {rows.count()} drop-3 rows with sugar in grams (\"12 g\") or a dd/MM/yyyy date "
                   f"are not in {P['silver']} (first missing code: {_first_code(missing)}). In your silver hop, strip the "
                   f"trailing g from sugars_100g and parse last_modified with both yyyy-MM-dd and dd/MM/yyyy, "
                   f"never in gold: lesson 6, section 4.")
    from delta import DeltaTable
    for t in ("nutriscore_by_category", "brands_sugar"):
        if not DeltaTable.isDeltaTable(spark, f"{P['gold']}/{t}"):
            out.append(f"no Delta table at {P['gold']}/{t} (gold): {LESSON['gold']}")
    return out


def gate_5(spark, P):
    from delta import DeltaTable
    cols = COLS + ["ecoscore_grade"]
    b = _stamped_bronze(spark, P, extra=["ecoscore_grade"]).select(cols)
    d4 = _drop(spark, P, 4).withColumnsRenamed({"sugar_100g": "sugars_100g"}).select(cols)
    out = []
    if (n := d4.subtract(b).count()):
        out.append(f"{n} of {d4.count()} drop-4 rows are not in {P['bronze']} with ecoscore_grade "
                   f"(first missing code: {_first_code(d4.subtract(b))}). Add ecoscore_grade to bronze on purpose, "
                   f"map sugar_100g back to sugars_100g, then append drop 4: lesson 7, section 4.")
    if not DeltaTable.forPath(spark, P["bronze"]).history().filter("operation = 'ADD COLUMNS'").count():
        out.append(f"{P['bronze']} history has no ADD COLUMNS commit (lesson 7, section 4: ALTER TABLE ... ADD COLUMNS)")
    return out


GATES = {2: gate_2, 3: gate_3, 4: gate_4, 5: gate_5}


# ---------- commands ----------

def cmd_next(force):
    h, p = handle(), prefix()
    P = paths(h, p)
    spark = spark_session(p.startswith("gs://"))
    if not exists(spark, P["landing"]):
        raise Refused(f"no landing bucket {P['landing']}: create it with Terraform (lesson 1)")
    n = current_drop(spark, P["landing"]) + 1
    if n > LAST_DROP:
        print(f"You have all {LAST_DROP} drops. Next: drop check silver")
        return 0
    if n in GATES and not force:
        try:
            gaps = GATES[n](spark, P)
        except Refused as e:
            gaps = [str(e)]
        except Exception as e:  # a Spark error is not a gate rule: show its first line, not the Java stack
            first = str(e).strip().splitlines()[0] if str(e).strip() else ""
            gaps = [(f"{type(e).__name__}: {first} (Spark failed while gate {n} read your tables, not a gate rule: "
                      "open the table path named in the error with spark.read.format('delta').load(<path>), "
                      "fix the hop that writes it, then run drop next again)")]
        if gaps:
            log(spark, P, h, n, "refused", gaps)
            print(f"Gate to drop {n} refused on {len(gaps)} rule(s), each line names the table and what it checked:")
            for g in gaps:
                print(f"  - {g}")
            print("Fix it and run drop next again, or drop next --force (logged, mention it in your PR).")
            return 1
    deliver(spark, P, n)
    event = "force" if force and n in GATES else "delivered"
    log(spark, P, h, n, event)
    if event == "force":
        print(f"Gate to drop {n} skipped with --force: logged for {h} at "
              f"{datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%M} UTC. Name it in your PR, and fix the "
              f"refused lines before the next gate, which reads the same tables.")
    print(f"Drop {n} is in {P['landing']}/drop{n}/. Next: run each hop you've built on it, landing to bronze first, "
          f"then go back to the lesson that asked for this drop.")
    return 0


def cmd_check_silver():
    from datacontract.data_contract import DataContract
    P = paths(handle(), prefix())
    spark = spark_session(P["silver"].startswith("gs://"))
    _table(spark, P["silver"], "silver").createOrReplaceTempView("silver_food_products")
    run = DataContract(data_contract_file=str(CONTRACT), spark=spark).test()
    for c in run.checks:
        print(f"[{c.result.value if c.result else 'unknown'}] {c.name}" + (f" -- {c.reason}" if c.reason else ""))
    if not run.has_passed():
        print("A failed check is a silver fix, not a contract edit: lesson 9, section 4.")
    return 0 if run.has_passed() else 1


def main(argv=None):
    ap = argparse.ArgumentParser(prog="drop")
    sub = ap.add_subparsers(dest="cmd", required=True)
    nxt = sub.add_parser("next", help="gate on your pipeline, then deliver the next drop to landing")
    nxt.add_argument("--force", action="store_true", help="deliver anyway; logged with your handle and the time")
    chk = sub.add_parser("check", help="run the data contract")
    chk.add_argument("what", choices=["silver"])
    a = ap.parse_args(argv)
    try:
        return cmd_next(a.force) if a.cmd == "next" else cmd_check_silver()
    except Refused as e:
        print(f"drop {a.cmd}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
