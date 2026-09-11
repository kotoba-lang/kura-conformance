(ns kura.conformance.probe
  "The Phase 0 measurement: does a shard written to each backend still read
  back, and how long did it take.

  ADR-2607299200 section 1 says the storage multiplier cannot be chosen without
  a measured node-loss rate, and section 8 says Phase 0 exists to measure it.
  This is that measurement. It is deliberately the least clever thing that
  produces the number: write a canary, read it back, compare the bytes, record
  the outcome with a timestamp.

  **What this can and cannot support.** A probe series measures *availability*
  — whether a backend answered correctly at a moment. Availability is not the
  node-loss rate: a backend that is briefly unreachable has lost nothing, and
  one that silently drops an object may answer every probe about every other
  object. So the honest output is `observed failures over N samples in a T-hour
  window`, and `kura.conformance.status` refuses to extrapolate an annual rate
  until the window is long enough to mean something. Quoting nines off a day of
  probes would be exactly the thing the rest of this project keeps declining
  to do.

  **The canary is rewritten every round, not written once.** A read-back of an
  object written days ago tests durability; a read-back of one written a second
  ago tests the path. Both matter and they are different measurements, so the
  probe carries both: a fresh canary per round, and a `stable` object written
  once at epoch zero and never rewritten."
  (:require [kura.node.async :as async]
            [kura.node.gf :as gf]))

(def stable-shard-id
  "Written once, never rewritten. Its read-back is the durability signal —
  the fresh canary only tells you the write/read path works right now."
  "kura-probe-stable/0/0")

(defn- canary-id [round]
  (str "kura-probe-canary/0/" (mod round 64)))

(defn- payload [seed n]
  (gf/->bytes (mapv #(mod (+ (* seed 131) (* % 17)) 256) (range n))))

(defn- timed>
  "Run `f>` and record wall-clock milliseconds alongside its outcome."
  [f>]
  (let [t0 (js/Date.now)]
    (-> (f>)
        (.then (fn [v] {:ok true :value v :ms (- (js/Date.now) t0)}))
        (.catch (fn [e] {:ok false :error (.-message e) :ms (- (js/Date.now) t0)})))))

(defn ensure-stable>
  "Write the stable object if it is not there.

  Distinguishes CREATED from REWROTE, and the distinction is the whole value of
  the field. Round 0 necessarily creates it — the object cannot have been there
  before the first probe — so counting that as a rewrite would put a durability
  event in the log on day one and leave it there forever, exactly the kind of
  number that gets quoted later by someone who did not read how it was
  produced. A rewrite after round 0 means the object was there and is not: the
  single most interesting event this measurement can produce."
  [store round]
  (-> (timed> #(async/-get-shard> store stable-shard-id))
      (.then (fn [r]
               (if (and (:ok r) (:value r))
                 {:present true :ms (:ms r)}
                 (-> (async/-put-shard!> store stable-shard-id (payload 7 4096))
                     (.then (fn [_] {:present false
                                     :created (zero? round)
                                     :rewrote (pos? round)}))
                     (.catch (fn [e] {:present false :created false :rewrote false
                                      :error (.-message e)}))))))))

(defn probe-backend>
  "One round against one backend."
  [[label store] round]
  (let [cid (canary-id round)
        body (payload round 4096)
        expect (gf/->vec body)]
    (-> (ensure-stable> store round)
        (.then (fn [stable]
                 (-> (timed> #(async/-put-shard!> store cid body))
                     (.then (fn [w]
                              (if-not (:ok w)
                                {:write w :read nil}
                                (-> (timed> #(async/-get-shard> store cid))
                                    (.then (fn [r]
                                             {:write w
                                              :read (assoc r :match
                                                           (and (:ok r)
                                                                (some? (:value r))
                                                                (= expect (gf/->vec (:value r)))))}))))))
                     (.then (fn [{:keys [write read]}]
                              {:backend (name label)
                               :round round
                               :ok (boolean (and (:ok write) (:match read)))
                               :write-ms (:ms write)
                               :read-ms (:ms read)
                               :stable-present (:present stable)
                               :stable-created (boolean (:created stable))
                               ;; True only when the object HAD been written
                               ;; and was gone. Round 0's creation is not this.
                               :stable-rewrote (boolean (:rewrote stable))
                               :error (or (:error write) (:error read)
                                          (when (and read (not (:match read)))
                                            "read-back did not match written bytes"))}))))))))

(defn round>
  "One probe round across every backend. Returns a record ready to append."
  [backends round at-iso]
  (-> (js/Promise.all (clj->js (map #(-> (probe-backend> % round)
                                         (.then clj->js))
                                    backends)))
      (.then (fn [rs]
               (let [rs (js->clj rs :keywordize-keys true)]
                 {:at at-iso
                  :round round
                  :backends rs
                  :all-ok (every? :ok rs)})))))
