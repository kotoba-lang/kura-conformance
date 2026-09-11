(ns kura.conformance.durability
  "End to end: store an object across real buckets, destroy shards, repair,
  read it back.

  Every other check in this repo tests a part. This one closes the loop the
  whole design exists to close — **lose shards and still have the object** —
  against real object stores at two providers, and it does the destroying
  itself so the result is not conditional on waiting for a real failure.

  What it proves that a unit test cannot: that `erasure`'s recovery plan, run
  through `kura.node.async` against services with their own consistency
  behaviour and their own idea of what a Range header means, actually
  reconstructs the original bytes. `erasure.codec-test` proves the algebra.
  This proves the algebra survives the network.

  **It deletes shards on purpose.** Only ones it wrote itself, under its own
  object id, in the Phase 0 buckets. Nothing here touches a shard it did not
  create.

  **Two providers, not the whole fleet.** A Worker cannot reach a node on a
  tailnet, so this proves the algebra survives the network across the two rented
  backends and nothing more. The same demonstration has been driven across all
  four domains — including deleting an entire domain's shards, the whole Fukuoka
  room, with identical bytes recovered — from inside the tailnet by
  `kura-node/script/multi_domain_durability.cljs`. That run is not schedulable
  from here, which is exactly why this one exists: what can be automated is
  narrower than what has been proved, and conflating them would let the narrower
  thing wear the wider claim."
  (:require [erasure.lrc :as lrc]
            [erasure.matrix :as matrix]
            [kura.node.async :as async]
            [kura.node.gf :as gf]))

(def layout
  "The target layout — k=16, n=26 — because it is the one whose minimum
  distance was established exhaustively (d=8, tolerates 7). Demonstrating
  repair on a configuration whose distance is only bounded would be
  demonstrating it on a guess."
  (lrc/layout {:k 16 :r 4 :g 6}))

(def ^:private shard-bytes 1024)

(defn- object-id [run] (str "kura-durability-" run))

(defn- shard-id [run i] (str (object-id run) "/0/" i))

(defn- source-shards
  "The k data shards of a synthetic object."
  [run]
  (mapv (fn [i]
          (gf/->bytes (mapv #(mod (+ (* run 71) (* i 131) (* % 17)) 256)
                            (range shard-bytes))))
        (range (:k layout))))

(defn- encode
  "Data shards plus local and global parity — the full n-shard set."
  [data]
  (into (vec data)
        (concat
         (map (fn [q] (gf/xor-shards (map #(nth data %) (lrc/group-members layout q))
                                     shard-bytes))
              (range (:l layout)))
         (map (fn [row] (gf/apply-row row data shard-bytes))
              (matrix/cauchy-rows (:k layout) (:g layout))))))

(defn- backend-for
  "Shard i lives on backend (i mod count). Round-robin stands in for
  `kura.placement` here: the placement function is exercised by its own tests,
  and what this route is proving is reconstruction, not dispersion."
  [backends i]
  (second (nth backends (mod i (count backends)))))

(defn- write-all> [backends run shards]
  (js/Promise.all
   (clj->js (map-indexed (fn [i s]
                           (async/-put-shard!> (backend-for backends i) (shard-id run i) s))
                         shards))))

(defn- read-shard> [backends run i]
  (-> (async/-get-shard> (backend-for backends i) (shard-id run i))
      (.catch (fn [_] nil))))

(defn- read-all> [backends run]
  (js/Promise.all (clj->js (map #(read-shard> backends run %) (range (:n layout))))))

(defn- destroy> [backends run idxs]
  (js/Promise.all
   (clj->js (map #(-> (async/-delete-shard!> (backend-for backends %) (shard-id run %))
                      (.catch (fn [_] false)))
                 idxs))))

(defn- cleanup> [backends run]
  (js/Promise.all
   (clj->js (map #(-> (async/-delete-shard!> (backend-for backends %) (shard-id run %))
                      (.catch (fn [_] false)))
                 (range (:n layout))))))

;; --- repair ----------------------------------------------------------------

(defn- run-step
  "Execute one plan step over the shards we have in hand."
  [shards {:keys [op target targets reads]}]
  (case op
    :local
    (assoc shards target (gf/xor-shards (map #(nth shards %) reads) shard-bytes))

    :recompute-global
    (assoc shards target
           (gf/apply-row (nth (matrix/cauchy-rows (:k layout) (:g layout))
                              (lrc/global-parity-index layout target))
                         (subvec shards 0 (:k layout))
                         shard-bytes))

    :global
    (let [m (mapv #(lrc/generator-row layout %) reads)
          inv (matrix/invert m)
          observed (mapv #(nth shards %) reads)
          recovered (mapv (fn [row] (gf/apply-row row observed shard-bytes)) inv)]
      (reduce (fn [acc j] (assoc acc j (nth recovered j))) shards targets))))

(defn- repair
  "Rebuild every missing shard from what survived, following `erasure`'s plan."
  [present]
  (let [erased (into #{} (keep-indexed (fn [i s] (when (nil? s) i)) present))
        plan (lrc/recovery-plan layout erased)]
    (when (:recoverable? plan)
      {:plan plan
       :shards (reduce run-step
                       (mapv #(or % (gf/alloc shard-bytes)) present)
                       (:steps plan))})))

;; --- the demonstration -----------------------------------------------------

(defn demonstrate>
  "Store, destroy, repair, verify. `kill` is the shard indices to delete."
  [backends run kill]
  (let [data (source-shards run)
        expect (mapv gf/->vec data)
        all (encode data)]
    (-> (write-all> backends run all)
        (.then (fn [_] (destroy> backends run kill)))
        (.then (fn [_] (read-all> backends run)))
        (.then (fn [got]
                 (let [present (mapv #(when (and % (pos? (gf/blength %))) %)
                                     (js->clj got))
                       actually-missing (into (sorted-set)
                                              (keep-indexed (fn [i s] (when (nil? s) i)) present))
                       r (repair present)]
                   (-> (cleanup> backends run)
                       (.then (fn [_]
                                {:layout (select-keys layout [:k :r :g :l :n])
                                 :object (object-id run)
                                 :shards-written (:n layout)
                                 :shards-destroyed (vec (sort kill))
                                 ;; Read back from the real stores, so this is
                                 ;; what the services actually said, not what
                                 ;; we asked them to do.
                                 :shards-observed-missing (vec actually-missing)
                                 :recoverable (boolean r)
                                 :repair-steps (when r
                                                 (mapv (fn [s] (select-keys s [:op :target :targets]))
                                                       (:steps (:plan r))))
                                 :reads-used (when r (count (:reads (:plan r))))
                                 ;; The claim, checked byte for byte.
                                 :data-recovered
                                 (boolean
                                  (and r (= expect (mapv gf/->vec
                                                         (subvec (:shards r) 0 (:k layout))))))
                                 :tolerated (lrc/max-tolerated-erasures layout)}))))))
        (.catch (fn [e]
                  (-> (cleanup> backends run)
                      (.then (fn [_] {:error (.-message e)}))))))))

(defn scenarios
  "The cases worth showing, and one that must fail.

  A demonstration that only ever succeeds proves nothing about where the edge
  is — so the last scenario destroys eight shards, which is past the measured
  distance, and the expected result is that recovery is refused rather than
  attempted and wrong."
  []
  [{:name "single shard lost — the 99% case"
    :kill [5] :expect-recoverable true}
   {:name "a whole local group plus its parity"
    :kill [0 1 2 3 16] :expect-recoverable true}
   {:name "seven arbitrary — the measured limit"
    :kill [0 1 2 3 4 5 6] :expect-recoverable true}
   {:name "eight — past the measured distance, must refuse"
    :kill [0 1 2 3 4 5 6 7] :expect-recoverable false}])
