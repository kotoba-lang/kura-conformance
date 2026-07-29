(ns kura.conformance.worker
  "Live conformance: the shard-store contract, against a real R2 bucket.

  `GET /conformance` runs `kura.node.async/verify>` and returns the result as
  JSON, so a conformance run is a URL anyone can fetch rather than a claim in
  a README. `GET /` describes what this is."
  (:require [kura.node.async :as async]
            [kura.node.r2 :as r2]
            [kura.node.store :as store]))

(defn- json [status body]
  (js/Response. (js/JSON.stringify (clj->js body) nil 2)
                #js {:status status
                     :headers #js {"content-type" "application/json; charset=utf-8"
                                   "cache-control" "no-store"}}))

(defn- store-for [env]
  (r2/open {:node-id "r2-phase0"
            :bucket (.-SHARDS ^js env)
            :prefix "kura"
            ;; Honest: one bucket in one account is one failure domain, however
            ;; many prefixes are carved out of it.
            :independence :shared-substrate
            :failure-domain {:provider "cloudflare-r2" :bucket "kura-phase0"}}))

(defn- handle [request env]
  (let [path (.-pathname (js/URL. (.-url request)))]
    (case path
      "/conformance"
      (let [s (store-for env)]
        (-> (async/run> s)
            (.then (fn [r]
                     (json (if (zero? (:failed r)) 200 500)
                           (assoc r
                                  :backend "cloudflare-r2 binding"
                                  :bucket "kura-phase0"
                                  :contract "kura.node.async/IAsyncShardStore"
                                  :descriptor (store/-descriptor s)))))))

      "/audit"
      ;; What a fleet of these is actually worth, which is the number the
      ;; durability argument turns on.
      (let [d (store/-descriptor (store-for env))]
        (js/Promise.resolve
         (json 200 (store/audit (vec (repeat 26 d)) 13))))

      (js/Promise.resolve
       (json 200 {:what "live conformance for the kura shard-store contract"
                  :why (str "the synchronous protocol silently reported every "
                            "read as absent when handed an async transport, and "
                            "the unit suite could not catch it because its fake "
                            "transport was synchronous too")
                  :routes ["/conformance" "/audit"]})))))

(def handler
  #js {:fetch (fn [request env _ctx] (handle request env))})
