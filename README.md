# BlackBoxQL
# Deterministic Query Engine with Sandboxed Execution
# Nim 2.x
#
# Features:
# - Declarative query AST
# - Deterministic execution
# - Execution limits (fuel-based)
# - Explain plans
# - Auditable results
#

import std/[tables, sequtils, strutils, hashes]

# ===============================
# Core Types
# ===============================

type
  ValueKind = enum
    vkInt, vkString

  Value = object
    case kind: ValueKind
    of vkInt:
      i: int
    of vkString:
      s: string

  Row = Table[string, Value]
  Dataset = seq[Row]

  PredicateOp = enum
    eq, gt, lt

  Predicate = object
    field: string
    op: PredicateOp
    value: Value

  Query = object
    selectFields: seq[string]
    whereClause: Predicate

  ExecResult = object
    rows: Dataset
    cost: int
    fingerprint: Hash

# ===============================
# Utility
# ===============================

proc hashValue(v: Value): Hash =
  case v.kind
  of vkInt: hash(v.i)
  of vkString: hash(v.s)

proc fingerprint(rows: Dataset): Hash =
  var h: Hash = 0
  for r in rows:
    for k, v in r:
      h = h !& hash(k)
      h = h !& hashValue(v)
  result = h

# ===============================
# Engine
# ===============================

type
  Engine = object
    fuelLimit: int

proc evalPredicate(row: Row, p: Predicate): bool =
  if not row.hasKey(p.field): return false
  let v = row[p.field]

  case (v.kind, p.value.kind)
  of (vkInt, vkInt):
    case p.op
    of eq: v.i == p.value.i
    of gt: v.i > p.value.i
    of lt: v.i < p.value.i
  of (vkString, vkString):
    case p.op
    of eq: v.s == p.value.s
    else: false
  else:
    false

proc execute(engine: Engine, data: Dataset, q: Query): ExecResult =
  var fuel = engine.fuelLimit
  var output: Dataset = @[]
  var cost = 0

  for row in data:
    if fuel <= 0:
      break

    fuel.dec
    cost.inc

    if evalPredicate(row, q.whereClause):
      var projected: Row
      for f in q.selectFields:
        if row.hasKey(f):
          projected[f] = row[f]
      output.add(projected)

  ExecResult(
    rows: output,
    cost: cost,
    fingerprint: fingerprint(output)
  )

# ===============================
# Explain Plan
# ===============================

proc explain(q: Query): string =
  "SCAN -> FILTER(" &
  q.whereClause.field & " " &
  $q.whereClause.op & ") -> PROJECT(" &
  q.selectFields.join(",") & ")"

# ===============================
# Demo
# ===============================

proc main() =
  var data: Dataset = @[]

  data.add({
    "id": Value(kind: vkInt, i: 1),
    "user": Value(kind: vkString, s: "alice"),
    "score": Value(kind: vkInt, i: 42)
  }.toTable)

  data.add({
    "id": Value(kind: vkInt, i: 2),
    "user": Value(kind: vkString, s: "bob"),
    "score": Value(kind: vkInt, i: 12)
  }.toTable)

  let q = Query(
    selectFields: @["id", "user"],
    whereClause: Predicate(
      field: "score",
      op: gt,
      value: Value(kind: vkInt, i: 20)
    )
  )

  let engine = Engine(fuelLimit: 100)
  let result = engine.execute(data, q)

  echo "Explain plan: ", explain(q)
  echo "Cost: ", result.cost
  echo "Fingerprint: ", result.fingerprint
  echo "Rows:"

  for r in result.rows:
    echo r

main()
