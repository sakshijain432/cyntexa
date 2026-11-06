# R3 - Claims quarantine pipeline
# Reads claims from bronze/raw Unity Catalog tables when present, falling back
# to the landing volume when they are not. Judges only the LATEST
# version of each claim (resubmissions), evaluates five quarantine rules
# (Q-A..Q-E), and splits claims into claims_clean and claims_quarantine.
# Nothing is silently dropped: every failing claim lands in claims_quarantine
# with a failed_rules array listing every rule it broke.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CLAIMS_SOURCE = "/Volumes/claims/raw/landing/claims_raw"
MEMBERS_SOURCE = "/Volumes/claims/raw/landing/members.csv"
HOSPITALS_SOURCE = "/Volumes/claims/raw/landing/hospitals.csv"

# Prefer existing bronze/raw Unity Catalog tables over reading the volume
# files directly; fall back to the landing volume only when no table exists.
CLAIMS_TABLES = ["claims.bronze.claims", "claims.raw.claims"]
MEMBERS_TABLES = ["claims.bronze.members", "claims.raw.members"]
HOSPITALS_TABLES = ["claims.bronze.hospitals", "claims.raw.hospitals"]


def _first_existing_table(names):
    """Return the first existing table name, or None to use the volume fallback."""
    for name in names:
        try:
            if spark.catalog.tableExists(name):
                return name
        except Exception:
            continue
    return None

# Columns carried through to claims_clean / claims_quarantine
CLAIM_COLUMNS = [
    "claim_id", "member_id", "policy_id", "hospital_id",
    "claim_type", "channel",
    "admission_date", "discharge_date", "submitted_ts", "settled_ts",
    "status", "diagnosis_code", "procedure_code", "pre_auth_id",
    "is_pre_existing", "billed_amount", "approved_amount",
    "rejection_code", "room_days", "room_rent_per_day",
]


# ---------------------------------------------------------------------------
# Source: bronze/raw table (preferred) or landing volume - no extra copy
# ---------------------------------------------------------------------------

@dp.temporary_view(
    comment="Claims read from claims.bronze.claims (or claims.raw.claims) when "
            "present, else the landing volume JSON; types normalized"
)
def claims_source():
    claims_table = _first_existing_table(CLAIMS_TABLES)
    if claims_table is not None:
        source = spark.read.table(claims_table)
    else:
        source = spark.read.format("json").load(CLAIMS_SOURCE)
    # The bronze table stores every column as string: normalize types so the
    # quarantine rules compare numbers, not lexicographic strings.
    return source.select(
        "claim_id", "member_id", "policy_id", "hospital_id",
        "claim_type", "channel",
        "admission_date", "discharge_date", "submitted_ts", "settled_ts",
        "status", "diagnosis_code", "procedure_code", "pre_auth_id",
        F.expr("try_cast(is_pre_existing AS boolean) AS is_pre_existing"),
        F.expr("try_cast(billed_amount AS double) AS billed_amount"),
        F.expr("try_cast(approved_amount AS double) AS approved_amount"),
        "rejection_code",
        F.expr("try_cast(room_days AS long) AS room_days"),
        F.expr("try_cast(room_rent_per_day AS long) AS room_rent_per_day"),
    )


# Reference data for the referential-integrity check (pipeline-private)

@dp.temporary_view()
def members_ref():
    table = _first_existing_table(MEMBERS_TABLES)
    if table is not None:
        return spark.read.table(table).select("member_id")
    return spark.read.option("header", "true").csv(MEMBERS_SOURCE).select("member_id")


@dp.temporary_view()
def hospitals_ref():
    table = _first_existing_table(HOSPITALS_TABLES)
    if table is not None:
        return spark.read.table(table).select("hospital_id")
    return spark.read.option("header", "true").csv(HOSPITALS_SOURCE).select("hospital_id")


# ---------------------------------------------------------------------------
# Rule evaluation on the latest version of each claim
# ---------------------------------------------------------------------------

@dp.materialized_view(
    private=True,
    comment="Latest version of every claim with the five quarantine rules "
            "evaluated (warn-only expectations passes_q_a..passes_q_e)",
)
@dp.expect("passes_q_a", "passes_q_a")
@dp.expect("passes_q_b", "passes_q_b")
@dp.expect("passes_q_c", "passes_q_c")
@dp.expect("passes_q_d", "passes_q_d")
@dp.expect("passes_q_e", "passes_q_e")
def claims_checked():
    claims = spark.read.table("claims_source")

    # Resubmitted claims: judge the latest version only (highest submitted_ts)
    latest = (
        claims.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("claim_id").orderBy(
                    F.to_timestamp("submitted_ts").desc()
                )
            ),
        )
        .where("rn = 1")
        .drop("rn")
    )

    checked = (
        latest
        # Q-A referential integrity: hospital and member must exist
        .join(
            F.broadcast(
                spark.read.table("members_ref").withColumnRenamed("member_id", "m_id")
            ),
            F.col("member_id") == F.col("m_id"),
            "left",
        )
        .join(
            F.broadcast(
                spark.read.table("hospitals_ref").withColumnRenamed("hospital_id", "h_id")
            ),
            F.col("hospital_id") == F.col("h_id"),
            "left",
        )
        .withColumn("adm", F.to_date("admission_date"))
        .withColumn("dis", F.to_date("discharge_date"))
        .withColumn("sub", F.to_timestamp("submitted_ts"))
        .withColumn("setl", F.to_timestamp("settled_ts"))
        .withColumn("los", F.datediff(F.col("dis"), F.col("adm")))
    )

    # Q-A: member_id and hospital_id both resolve to existing records
    passes_q_a = F.coalesce(
        F.col("m_id").isNotNull() & F.col("h_id").isNotNull(), F.lit(False)
    )

    # Q-B: no impossible dates - all dates parse, admission <= discharge,
    # claim submitted on/after admission, settled (if present) on/after submitted
    passes_q_b = F.coalesce(
        F.col("adm").isNotNull()
        & F.col("dis").isNotNull()
        & F.col("sub").isNotNull()
        & (F.col("adm") <= F.col("dis"))
        & (F.col("sub").cast("date") >= F.col("adm"))
        & (F.col("setl").isNull() | (F.col("setl") >= F.col("sub"))),
        F.lit(False),
    )

    # Q-C: amount sanity - billed present and non-negative; approved, when
    # present, non-negative and not exceeding the billed amount
    passes_q_c = F.coalesce(
        F.col("billed_amount").isNotNull()
        & (F.col("billed_amount") >= 0)
        & (
            F.col("approved_amount").isNull()
            | (
                (F.col("approved_amount") >= 0)
                & (F.col("approved_amount") <= F.col("billed_amount"))
            )
        ),
        F.lit(False),
    )

    # Q-D: status consistency - settled must carry settled_ts, rejected must
    # carry an RJ-* rejection_code, and no other status may carry either field
    passes_q_d = F.coalesce(
        ((F.col("status") != "settled") | F.col("settled_ts").isNotNull())
        & (
            (F.col("status") != "rejected")
            | (
                F.col("rejection_code").isNotNull()
                & F.col("rejection_code").rlike("^RJ-")
            )
        )
        & ((F.col("status") == "rejected") | F.col("rejection_code").isNull())
        & ((F.col("status") == "settled") | F.col("settled_ts").isNull()),
        F.lit(False),
    )

    # Q-E: room math - room_days must match the length of stay. Hospitals bill
    # the admission day, so a stay of N nights is billed N or N+1 room days.
    passes_q_e = F.coalesce(
        F.col("room_days").isNotNull()
        & F.col("los").isNotNull()
        & (F.col("room_days") >= F.col("los"))
        & (F.col("room_days") <= F.col("los") + 1),
        F.lit(False),
    )

    return (
        checked
        .withColumn("passes_q_a", passes_q_a)
        .withColumn("passes_q_b", passes_q_b)
        .withColumn("passes_q_c", passes_q_c)
        .withColumn("passes_q_d", passes_q_d)
        .withColumn("passes_q_e", passes_q_e)
        # A claim can fail multiple rules: list every rule broken
        .withColumn(
            "failed_rules",
            F.expr(
                "filter(array("
                "CASE WHEN passes_q_a THEN NULL ELSE 'Q-A' END, "
                "CASE WHEN passes_q_b THEN NULL ELSE 'Q-B' END, "
                "CASE WHEN passes_q_c THEN NULL ELSE 'Q-C' END, "
                "CASE WHEN passes_q_d THEN NULL ELSE 'Q-D' END, "
                "CASE WHEN passes_q_e THEN NULL ELSE 'Q-E' END"
                "), x -> x IS NOT NULL)"
            ),
        )
        .select(
            *CLAIM_COLUMNS,
            "passes_q_a", "passes_q_b", "passes_q_c", "passes_q_d", "passes_q_e",
            "failed_rules",
        )
    )


# ---------------------------------------------------------------------------
# Split: clean claims vs quarantined claims
# ---------------------------------------------------------------------------

@dp.materialized_view(
    comment="Claims that pass all five quarantine rules (Q-A..Q-E)"
)
def claims_clean():
    return (
        spark.read.table("claims_checked")
        .where("size(failed_rules) = 0")
        .select(*CLAIM_COLUMNS)
    )


@dp.materialized_view(
    comment="Claims failing at least one quarantine rule, with failed_rules "
            "listing every rule broken"
)
def claims_quarantine():
    return (
        spark.read.table("claims_checked")
        .where("size(failed_rules) > 0")
        .select(*CLAIM_COLUMNS, "failed_rules")
    )
