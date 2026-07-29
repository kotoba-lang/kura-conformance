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

## Build and deploy

```bash
npm install
node ../../../scripts/resource-guard.mjs run build -- npx shadow-cljs release worker
npx wrangler deploy
```

Source paths are sibling west checkouts (`../kura-node/src` etc.), the pattern
`kotobase-protocols-worker` uses. Builds go through the superproject's resource
governor because concurrent agent builds on this machine contend for CPU and
shadow's parallel compile aborts under it.

## License

MIT.
