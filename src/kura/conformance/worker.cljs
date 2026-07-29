(ns kura.conformance.worker
  "Live conformance and fleet audit for the kura shard-store contract, against
  real object stores.

  `GET /conformance` runs `kura.node.async/verify>` against each backend, so a
  conformance run is a URL anyone can fetch rather than a claim in a README.

  `GET /audit` is the one that matters for the durability argument. A code that
  tolerates 13 arbitrary losses is worth nothing if all 32 shards sit in one
  bucket. The fleet here is deliberately **two providers** — Cloudflare R2 and
  Backblaze B2 — because one provider is one failure domain however many
  prefixes are carved out of it, and ADR-2607299200 section 1's entire model
  assumes shard losses are independent."
  (:require [kura.node.async :as async]
            [kura.node.crypto-noble :as nc]
            [kura.node.http-fetch :as http]
            [kura.node.r2 :as r2]
            [kura.conformance.durability :as dur]
            [kura.conformance.probe :as probe]
            [kura.conformance.status :as status]
            [kura.node.s3-async :as s3a]
            [kura.node.store :as store]))

(defn- json [status body]
  (js/Response. (js/JSON.stringify (clj->js body) nil 2)
                #js {:status status
                     :headers #js {"content-type" "application/json; charset=utf-8"
                                   "cache-control" "no-store"}}))

(def ^:private b2-host "s3.us-west-004.backblazeb2.com")

(defn- r2-store [env]
  (r2/open {:node-id "r2-phase0"
            :bucket (.-SHARDS ^js env)
            :prefix "kura"
            :independence :shared-substrate
            :failure-domain {:provider "cloudflare-r2" :bucket "kura-phase0"}}))

(defn- b2-store [env]
  (s3a/open {:node-id "b2-phase0"
             :endpoint (str "https://" b2-host)
             :host b2-host
             :bucket "kura-phase0-b2"
             :region "us-west-004"
             :key-id (.-B2_KEY_ID ^js env)
             :secret (.-B2_APP_KEY ^js env)
             :prefix "kura"
             ;; A different company, different hardware, different control
             ;; plane. This declaration is what the durability model rests on,
             ;; and it is the operator's claim to stand behind — the
             ;; conformance suite deliberately cannot check it.
             :independence :shared-provider
             :failure-domain {:provider "backblaze-b2" :bucket "kura-phase0-b2"}
             :http (http/fetch-http)
             :crypto (nc/noble-crypto)
             :now-fn http/now-iso}))

(defn- backends
  "Every backend the harness can reach. B2 appears only when its credentials
  are configured, so a deploy without them degrades to a single-provider run
  that `/audit` will then honestly report as one failure domain."
  [env]
  (cond-> [[:r2 (r2-store env)]]
    (and (.-B2_KEY_ID ^js env) (.-B2_APP_KEY ^js env))
    (conj [:b2 (b2-store env)])))

(defn- conformance> [env]
  (-> (js/Promise.all
       (clj->js (map (fn [[k s]]
                       (-> (async/run> s)
                           (.then (fn [r]
                                    (clj->js
                                     (assoc r :backend (name k)
                                            :descriptor (store/-descriptor s)))))))
                     (backends env))))
      (.then (fn [rs]
               (let [rs (js->clj rs :keywordize-keys true)
                     failed (reduce + 0 (map :failed rs))]
                 (json (if (zero? failed) 200 500)
                       {:results rs
                        :total-failed failed
                        :contract "kura.node.async/IAsyncShardStore"}))))))

(defn- fleet-audit
  "What a `shards`-wide placement over these providers is actually worth.

  Shards are dealt round-robin across the declared backends, which is what a
  placement under `kura.placement`'s domain caps approximates. `largest-domain`
  is the number that has to stay at or under the code's tolerance — otherwise
  one provider's bad day is the object's."
  [env tolerated shards]
  (let [ds (mapv (fn [[_ s]] (store/-descriptor s)) (backends env))
        n (count ds)
        fleet (mapv (fn [i]
                      (let [d (nth ds (mod i n))]
                        (assoc d :node-id (str (:node-id d) "-" i))))
                    (range shards))]
    (assoc (store/audit fleet tolerated)
           :providers (mapv #(get-in % [:failure-domain :provider]) ds)
           :shards shards)))

(def ^:private log-key
  "Versioned because the v1 series carried records written before
  `probe/ensure-stable>` distinguished CREATED from REWROTE, so its round-0
  creation reads as a durability event forever. A schema change deserves a new
  series rather than a silently mixed one.

  CORRECTION: an earlier version of this comment blamed the failed delete-and-
  reseed on R2 eventual consistency. That was wrong. `wrangler r2 object
  delete` defaults to the LOCAL simulator; without `--remote` it never touched
  the bucket, so of course the log did not reset. The versioned key is still
  the right call, but for the reason above and not the one first given."
  "status/probe-log.v2.jsonl")

(def ^:private max-log-rounds
  "Rounds kept in the probe log.

  The log is read-modify-write on every round, so an unbounded file means the
  bytes written grow quadratically in the number of rounds — at 48 rounds a day
  that is slow enough to ignore for months and exactly the kind of thing nobody
  notices until it is large. 2000 rounds is ~6 weeks, comfortably past the
  168-hour window `status/min-window-hours` needs before it will report a rate."
  2000)

(defn- append-probe>
  "Append one round to the probe log in R2.

  Read-modify-write, which is racy under concurrency and is fine here: the
  cron fires one round at a time and a lost round costs one sample. Using a
  transactional store for a measurement log would be spending the complexity
  budget in the wrong place — but the raciness is written down rather than
  discovered."
  [env record]
  (let [b (.-SHARDS ^js env)]
    (-> (.get b log-key)
        (.then (fn [o] (if o (.text ^js o) "")))
        (.then (fn [prev]
                 (let [lines (vec (remove empty? (.split prev "\n")))
                       kept (if (> (count lines) max-log-rounds)
                              (subvec lines (- (count lines) max-log-rounds))
                              lines)
                       next-log (str (clojure.string/join "\n"
                                                          (conj kept
                                                                (js/JSON.stringify (clj->js record))))
                                     "\n")]
                   (.put b log-key next-log))))
        (.then (fn [_] record)))))

(defn- run-probe> [env]
  (let [now (.toISOString (js/Date.))]
    (-> (.get (.-SHARDS ^js env) log-key)
        (.then (fn [o] (if o (.text ^js o) "")))
        (.then (fn [prev] (count (remove empty? (.split prev "\n")))))
        (.then (fn [round] (probe/round> (backends env) round now)))
        (.then (fn [rec] (append-probe> env rec))))))

(defn- status> [env]
  (-> (.get (.-SHARDS ^js env) log-key)
      (.then (fn [o] (if o (.text ^js o) "")))
      (.then (fn [text]
               (json 200 (assoc (status/summarise text (.toISOString (js/Date.)))
                                :fleet (fleet-audit env 13 32)
                                ;; Which series this is. Verifying a schema
                                ;; change by rapid manual probing does not work
                                ;; — Cloudflare propagates a deploy across edges
                                ;; over some seconds, and /probe can force rounds
                                ;; faster than that, so consecutive checks land
                                ;; on different Worker versions writing different
                                ;; keys. Reporting the key makes that visible
                                ;; instead of confusing.
                                :log-key log-key))))))

(defn- handle [request env]
  (let [path (.-pathname (js/URL. (.-url request)))]
    (case path
      "/conformance" (conformance> env)

      "/durability"
      ;; Store, destroy shards, repair, verify — against the real buckets.
      ;; Runs every scenario including the one that must fail, because a
      ;; demonstration that only ever succeeds says nothing about where the
      ;; edge is.
      (let [bs (backends env)
            base (js/Math.floor (/ (js/Date.now) 1000))]
        (-> (js/Promise.all
             (clj->js (map-indexed
                       (fn [i sc]
                         (-> (dur/demonstrate> bs (+ base i) (:kill sc))
                             (.then (fn [r]
                                      (clj->js
                                       (assoc r :scenario (:name sc)
                                              :expected-recoverable (:expect-recoverable sc)
                                              :as-expected
                                              (= (boolean (:expect-recoverable sc))
                                                 (boolean (:recoverable r)))))))))
                       (dur/scenarios))))
            (.then (fn [rs]
                     (let [rs (js->clj rs :keywordize-keys true)
                           bad (remove :as-expected rs)]
                       (json (if (empty? bad) 200 500)
                             {:scenarios rs
                              :all-as-expected (empty? bad)
                              :providers (mapv (fn [[k _]] (name k)) bs)
                              :note (str "shards are destroyed on purpose, only ones "
                                         "this route wrote, and cleaned up after")}))))))

      "/probe"
      ;; Also reachable by hand, so a round can be forced when something looks
      ;; wrong rather than waiting for the schedule.
      (-> (run-probe> env) (.then (fn [r] (json 200 r))))

      "/status" (status> env)

      "/audit"
      (js/Promise.resolve
       (json 200 {:launch-layout (fleet-audit env 13 32)
                  :target-layout (fleet-audit env 7 26)
                  :note (str "largest-domain must stay at or under tolerated. "
                             "One provider is one failure domain however many "
                             "prefixes are carved out of it.")}))

      (js/Promise.resolve
       (json 200 {:what "live conformance and fleet audit for the kura shard-store contract"
                  :why (str "two bugs got past the unit suite in a row, both "
                            "the same shape: a test that supplies its own world "
                            "agrees with itself. This one supplies none.")
                  :routes ["/conformance" "/durability" "/audit" "/status" "/probe"]})))))

(def handler
  #js {:fetch (fn [request env _ctx] (handle request env))
       ;; The Phase 0 measurement runs on a schedule, because a series taken
       ;; only when somebody remembers to look is a series that samples
       ;; attention rather than availability.
       :scheduled (fn [_event env ctx]
                    (.waitUntil ^js ctx (run-probe> env)))})
