# kura-conformance

Runs the [kura](https://github.com/kotoba-lang/kura-node) shard-store contract
against a **real Cloudflare R2 bucket**, inside a Worker, and returns the
result as JSON.

**https://kura-conformance.04-feasts-minded.workers.dev/conformance** — 19/19.

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
- `/audit` — what a fleet of 26 of these backends is actually worth:

  ```json
  {"backends": 26, "effective-domains": 1, "largest-domain": 26,
   "tolerated": 13, "survivable?": false,
   "note": "one domain holds 26 shards but the code tolerates 13 —
            durability is that domain's, not the code's"}
  ```

  One bucket is one failure domain however many prefixes are carved out of it.
  The harness says that about **itself**, which is the point: this is a
  measurement rig, not a durable deployment.

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
