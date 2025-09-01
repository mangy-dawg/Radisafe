# Copy a table from ProjectManagement to Dev using Azure AD Interactive auth (popup)
# - Prompts an AAD sign-in window with the username prefilled
# - Creates the target schema/table (incl. IDENTITY) if missing
# - Copies data in batches, preserving identity values (uses SET IDENTITY_INSERT)
# - Handles computed columns (creates them from their definitions, but does NOT insert into them)
#
# Prereqs:
#   pip install pyodbc
#   ODBC Driver 17 for SQL Server installed
#   Your AAD identity must have: SELECT on ProjectManagement.dim.Employees, and in Dev: CREATE TABLE on schema [dim] + INSERT.
#
# Notes:
#   - You may see the AAD popup twice (once per DB). Token reuse usually avoids the second prompt if close in time.
#   - Indexes/PKs/defaults/foreign keys are NOT recreated here. Add them after the copy if needed.

import pyodbc

SERVER = "radiancesqlserver.database.windows.net"   # e.g., "myserver.database.windows.net"
SRC_DB = "ProjectManagement"
DST_DB = "Dev"
SCHEMA = "dim"
TABLE = "Employees"
AAD_USER = "navpm@radiancerenewables.com"  # prefill this in the popup

# Build an AAD Interactive connection string that triggers the sign-in popup
BASE = "Driver={ODBC Driver 17 for SQL Server};Encrypt=yes;TrustServerCertificate=no;Authentication=ActiveDirectoryInteractive;UID=" + AAD_USER + ";"

src_cnx = pyodbc.connect(f"{BASE}Server=tcp:{SERVER},1433;Database={SRC_DB};", timeout=30, autocommit=False)
dst_cnx = pyodbc.connect(f"{BASE}Server=tcp:{SERVER},1433;Database={DST_DB};", timeout=30, autocommit=False)

# --- 1) Read source schema, including identity & computed definitions ---
meta_sql = f"""
SELECT
  c.column_id,
  c.name               AS col_name,
  t.name               AS data_type,
  c.max_length,
  c.[precision],
  c.scale,
  c.is_nullable,
  c.is_identity,
  c.is_computed,
  cc.definition        AS computed_definition,
  cc.is_persisted      AS computed_persisted
FROM sys.columns c
JOIN sys.types   t  ON c.user_type_id = t.user_type_id
JOIN sys.tables  tb ON c.object_id   = tb.object_id
JOIN sys.schemas s  ON tb.schema_id  = s.schema_id
LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
WHERE s.name = ? AND tb.name = ?
ORDER BY c.column_id;
"""

src_cur = src_cnx.cursor()
cols = src_cur.execute(meta_sql, SCHEMA, TABLE).fetchall()

# --- Fetch primary key metadata from source ---
pk_info_sql = """
SELECT kc.unique_index_id AS index_id, i.type AS index_type
FROM sys.key_constraints kc
JOIN sys.tables tb ON kc.parent_object_id = tb.object_id
JOIN sys.schemas s ON tb.schema_id = s.schema_id
JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
WHERE kc.type = 'PK' AND s.name = ? AND tb.name = ?;
"""

pk_cols_sql = """
SELECT c.name AS col_name, ic.is_descending_key, ic.key_ordinal
FROM sys.index_columns ic
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN sys.tables tb ON tb.object_id = ic.object_id
JOIN sys.schemas s ON tb.schema_id = s.schema_id
WHERE s.name = ? AND tb.name = ? AND ic.index_id = ?
ORDER BY ic.key_ordinal;
"""

pk_row = src_cur.execute(pk_info_sql, SCHEMA, TABLE).fetchone()
pk_cols = []
pk_is_clustered = False
if pk_row:
    pk_is_clustered = (pk_row.index_type == 1)  # 1 = CLUSTERED
    for r in src_cur.execute(pk_cols_sql, SCHEMA, TABLE, pk_row.index_id).fetchall():
        pk_cols.append((r.col_name, bool(r.is_descending_key)))

# Helpers to format SQL types from metadata

def format_sql_type(r):
    dt = (r.data_type or "").lower()
    # Normalize legacy LOBs to MAX types
    if dt == "text":
        return "varchar(MAX)"
    if dt == "ntext":
        return "nvarchar(MAX)"
    if dt == "image":
        return "varbinary(MAX)"

    if dt in ("varchar", "nvarchar", "varbinary"):
        if r.max_length == -1:
            return f"{dt}(MAX)"
        # max_length is in bytes; nvarchar stores 2 bytes/char
        if dt == "nvarchar":
            return f"nvarchar({r.max_length // 2})"
        else:
            return f"{dt}({r.max_length})"

    if dt in ("char", "nchar", "binary"):
        if dt == "nchar":
            return f"nchar({r.max_length // 2})"
        else:
            return f"{dt}({r.max_length})"

    if dt in ("decimal", "numeric"):
        return f"{dt}({r.precision},{r.scale})"

    if dt in ("datetime2", "datetimeoffset", "time"):
        s = r.scale if r.scale is not None and r.scale != 0 else 7
        return f"{dt}({s})"

    # Common types: int, bigint, bit, float, real, date, smalldatetime, datetime, uniqueidentifier, xml, money, etc.
    return dt

# --- 2) Create schema & table in Dev (computed columns supported) ---
dst_cur = dst_cnx.cursor()

# Ensure schema exists
schema_exists = dst_cur.execute("SELECT 1 FROM sys.schemas WHERE name = ?", SCHEMA).fetchone()
if not schema_exists:
    dst_cur.execute(f"CREATE SCHEMA [{SCHEMA}];")
    dst_cnx.commit()

# Build column definitions
col_defs = []
insertable_cols = []  # exclude computed columns from INSERT
has_identity = False

for r in cols:
    col_name = f"[{r.col_name}]"
    if r.is_computed:
        # Computed column: AS (definition) [PERSISTED]
        definition = r.computed_definition or ""
        persisted = " PERSISTED" if r.computed_persisted else ""
        col_defs.append(f"{col_name} AS {definition}{persisted}")
        # not insertable
        continue

    sql_type = format_sql_type(r)
    identity = " IDENTITY(1,1)" if r.is_identity else ""
    nullability = " NULL" if r.is_nullable else " NOT NULL"
    col_defs.append(f"{col_name} {sql_type}{identity}{nullability}")

    insertable_cols.append(col_name)
    if r.is_identity:
        has_identity = True

create_sql = f"CREATE TABLE [{SCHEMA}].[{TABLE}] (\n  " + ",\n  ".join(col_defs) + "\n);"

# Drop target table if it exists, then recreate
dst_cur.execute(f"""
IF OBJECT_ID(N'[{SCHEMA}].[{TABLE}]','U') IS NOT NULL
    DROP TABLE [{SCHEMA}].[{TABLE}];
""")
dst_cnx.commit()

# Recreate the table from the source schema
dst_cur.execute(create_sql)
dst_cnx.commit()

# --- 3) Copy data in batches ---
BATCH = 5000
src_select_cols = ", ".join(insertable_cols)  # select only insertable (non-computed) columns
src_select_sql = f"SELECT {src_select_cols} FROM [{SCHEMA}].[{TABLE}]"

src_cur2 = src_cnx.cursor()
src_cur2.execute(src_select_sql)

placeholders = ", ".join(["?"] * len(insertable_cols))
insert_sql = f"INSERT INTO [{SCHEMA}].[{TABLE}] ({src_select_cols}) VALUES ({placeholders})"

dst_cur.fast_executemany = True

try:
    if has_identity:
        dst_cur.execute(f"SET IDENTITY_INSERT [{SCHEMA}].[{TABLE}] ON;")

    rows_copied = 0
    while True:
        rows = src_cur2.fetchmany(BATCH)
        if not rows:
            break
        dst_cur.executemany(insert_sql, rows)
        dst_cnx.commit()
        rows_copied += len(rows)

finally:
    if has_identity:
        dst_cur.execute(f"SET IDENTITY_INSERT [{SCHEMA}].[{TABLE}] OFF;")
        dst_cnx.commit()

# --- 4) Create primary key in Dev to match source (if present) ---
if pk_cols:
    exists_pk = dst_cur.execute("""
        SELECT 1
        FROM sys.key_constraints kc
        JOIN sys.tables tb ON kc.parent_object_id = tb.object_id
        JOIN sys.schemas s ON tb.schema_id = s.schema_id
        WHERE kc.type = 'PK' AND s.name = ? AND tb.name = ?;
    """, SCHEMA, TABLE).fetchone()
    if not exists_pk:
        cols_ddl = ", ".join([f"[{name}] {'DESC' if is_desc else 'ASC'}" 
                      for name, is_desc in pk_cols])

        pk_name = f"PK_{SCHEMA}_{TABLE}"
        clustered = "CLUSTERED" if pk_is_clustered else "NONCLUSTERED"
        dst_cur.execute(f"ALTER TABLE [{SCHEMA}].[{TABLE}] ADD CONSTRAINT [{pk_name}] PRIMARY KEY {clustered} ({cols_ddl});")
        dst_cnx.commit()

print(f"Copied {rows_copied} rows to {DST_DB}.{SCHEMA}.{TABLE}")
