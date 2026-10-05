# Day 17 — Window Functions

`row_number`/`rank`/`dense_rank`, partitioning by department/customer/route,
finding the latest record per customer, `lag`/`lead`, and a scenario
combining top-N-per-group with latest-record-per-entity.

## Run

```bash
sbt "run local[4]"
```

## Data

- `employees.csv` — 37 employees across 5 departments, with a deliberate
  salary tie in Engineering to show rank vs dense_rank behaving differently
- `policies.csv` — insurance policies for 25 customers, 1-4 each
- `routes.csv` — stops along 3 bus routes with arrival times and distance
- `students.csv` — 51 students across 4 courses, with a 3-way score tie
  in Mathematics

## What it covers

1 & 2. **row_number / rank / dense_rank, partitioned by department**:
   a window spec is `PARTITION BY + ORDER BY`; partitioning restarts the
   numbering for every department. All three only differ on ties:
   `row_number` always gives unique numbers, `rank` lets ties share a
   value and then skips ahead, `dense_rank` lets ties share a value with
   no gap afterward.
3. **Latest record per customer**: `partitionBy("customerId")
   .orderBy(col("effectiveDate").desc)`, `row_number()`, keep `rn == 1`.
   `row_number` (not `rank`) is deliberate — it guarantees exactly one row
   per customer even if two records share the same date.
4. **lag/lead, partitioned by route**: `lag` looks back one row within
   the partition's order (null on the first row of each partition),
   `lead` looks forward (null on the last). Used here to pull in the
   previous/next stop and compute minutes between consecutive stops —
   the standard pattern for a delta between consecutive rows, no
   self-join needed.
5. **Scenario**: top 3 students per course via `dense_rank` (so a tie for
   3rd place keeps *all* tied students, rather than row_number's arbitrary
   cutoff), and the latest policy per customer reused to check how many
   customers currently have an ACTIVE policy as their most recent one.

## Push to GitHub

```bash
git init && git add . && git commit -m "Day 17: Window functions"
git branch -M main
git remote add origin https://github.com/seemran13/day17-window-functions.git
git push -u origin main
```
