# kura-conformance

Runs the [kura](https://github.com/kotoba-lang/kura-node) shard-store contract
against a **real Cloudflare R2 bucket**, inside a Worker, and returns the
result as JSON.

**https://kura-conformance.04-feasts-minded.workers.dev/conformance** — two
providers, 19/19 each: Cloudflare R2 (binding) and Backblaze B2 (SigV4 over
`kura.node.s3-async`).

## Why a live harness exists at all

Two bugs got past `kura-node`'s unit suite in a row, and they are the same
shape: **a test that supplies its own world agrees with itself.**

1. **The synchronous `IShardStore` protocol cannot survive an async
   transport.** Every host a node runs on has async I/O. `s3/send!` returned
   the transport's value, `ok?` read `:status` off it, and a promise has no
   `:status` — so every read reported the shard absent and every write
   reported success. The suite passed because its fake transport was
   synchronous.

2. **`:advanced` renames un-inferred property reads.** `(.-objects res)` on the
   R2 list result became `.-Xa` and returned `undefined`, so listing came back
   empty while eighteen other assertions passed. No unit test runs through
   `:advanced`.

Neither is findable by a test that mocks the world. This one does not: real
Worker, real bucket, real optimizer output.

## Routes

- `/conformance` — runs `kura.node.async/verify>` against R2. Returns
  `{passed, failed, failures, total}` plus the backend's declared descriptor.
  HTTP 200 when clean, 500 when not, so it works as a check.
- `/audit` — what a real placement over these providers is actually worth,
  and it currently says **no**:

  | layout | shards | tolerates | domains | need | largest | survivable |
  |---|---|---|---|---|---|---|
  | launch | 32 | 13 | 2 | ≥3 | 16 | ✗ |
  | target | 26 | 7 | 2 | ≥4 | 13 | ✗ |

  Two providers spread 32 shards 16-and-16, and 16 is more than 13.
  `ceil(shards / tolerated)` is the minimum number of genuinely independent
  failure domains; until the fleet has that many, the code's tolerance is
  decoration. **Turning "spread it across providers" into an integer that is
  either satisfied or not is the entire job of this route.**

  A third and fourth provider close it. Neither is a code change.

## Durability, demonstrated rather than argued

`/durability` stores an object across both providers, **destroys shards on
purpose**, repairs from what survived, and compares the recovered bytes to the
originals. Live, against real buckets:

| scenario | destroyed | plan | reads | recovered |
|---|---|---|---|---|
| single shard — the 99% case | 1 | local | **4** | byte-for-byte |
| a whole local group plus its parity | 5 | global + local | 20 | byte-for-byte |
| seven arbitrary — the measured limit | 7 | global | 16 | byte-for-byte |
| eight — past the measured distance | 8 | **refused** | — | correctly not attempted |

The last row is the important one. A demonstration that only ever succeeds says
nothing about where the edge is, so one scenario destroys eight shards — past
the exhaustively measured minimum distance — and the expected result is that
recovery is **refused** rather than attempted and silently wrong.

`erasure.codec-test` proves the algebra. This proves the algebra survives the
network: two services with their own consistency behaviour and their own idea
of what a Range header means. It deletes only shards it wrote itself, under its
own object id, and cleans up after.

## The Phase 0 measurement

`/probe` writes a canary to every backend, reads it back, compares the bytes,
and appends the outcome to a log in R2. A cron trigger runs it every 30
minutes, because a series taken only when somebody remembers to look samples
attention rather than availability.

`/status` summarises it, and the refusals are the point:

- **No annual rate from a short window.** `:node-loss-rate` stays
  `:insufficient-window` until the log covers 168 hours, and says how much
  longer it needs. A rate estimated from a few hours of probes is noise wearing
  the clothes of a fact.
- **No nines.** Availability is not durability. The only durability signal here
  is `stable-object-lost` — one object per backend, written once and never
  rewritten — and one object is not a durability measurement either.
- **Failure reasons, not just counts.** A count says something broke; the
  reason says whether it was the network, the service, or us.

Early numbers (two backends, minutes of window, therefore not a claim about
anything): r2 median read ~45 ms, b2 ~358 ms, no failures observed.

### A measurement artifact, and why the series is versioned

Round 0 necessarily *creates* the stable object — it cannot have been there
before the first probe. The first implementation counted that as a rewrite,
which put a durability event in the log on day one and would have left it there
forever, to be quoted later by someone who did not read how it was produced.
Fixed by distinguishing CREATED from REWROTE.

The log key is versioned (`probe-log.v2.jsonl`) rather than deleted and
re-seeded, because a schema change deserves a new series rather than a silently
mixed one.

**A correction.** An earlier version of this README blamed the failed
delete-and-reseed on R2 eventual consistency. That was wrong, and wrong in a way
worth keeping on the page: `wrangler r2 object delete` **defaults to the local
simulator**. Without `--remote` it never touched the bucket, so of course the
log did not reset — and the same omission made `r2 object get` report every
object as 0 bytes, which was read as a fetch race. Two mis-diagnoses from one
missing flag. The versioned key is still the right call; the reason first given
for it was not.

`/status` reports the key it read, because verifying a change by rapid manual
probing genuinely does not work — a deploy propagates across edges over some
seconds and `/probe` can force rounds faster than that.

### Cost

Deliberately near zero, and measured rather than assumed:

| | |
|---|---|
| probe log | ~1.5 KB, capped at 2000 rounds (~6 weeks, ~360 KB) |
| canaries | 4 KiB each, 64 rotating, per provider |
| stable object | 4 KiB per provider |
| cron | 48 rounds/day, a handful of operations each |

Under a megabyte in total, against 10 GB free tiers on both R2 and B2 and 100k
Worker requests a day. The log is read-modify-write per round, so an unbounded
file would grow the bytes written quadratically — slow enough to ignore for
months, which is exactly the kind of thing nobody notices until it is large.
Hence the cap.

## Build and deploy

```bash
npm install
node ../../../scripts/resource-guard.mjs run build -- amu compile --target wasm32-browser worker
npx wrangler deploy
```

Source paths are sibling west checkouts (`../kura-node/src` etc.), the pattern
`kotobase-protocols-worker` uses. Builds go through the superproject's resource
governor because concurrent agent builds on this machine contend for CPU and
shadow's parallel compile aborts under it.

## License

MIT.
