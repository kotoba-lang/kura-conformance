(ns kura.conformance.status
  "Reading the probe log back, and refusing to say more than it supports.

  The whole reason kura has a public page is that a storage network's
  durability claim is unfalsifiable until someone publishes which numbers came
  from a measurement. This namespace is where that discipline either holds or
  quietly stops holding, so the refusals are the important part:

  - **No annual rate from a short window.** An estimator over a few hours of
    probes has enormous variance; the point estimate would look like a fact and
    be noise. `:node-loss-rate` stays `:insufficient-window` until the log
    covers `min-window-hours`, and says how much longer it needs.
  - **No nines.** Availability is not durability. A backend that answered every
    probe may still have silently dropped an object it was never asked about;
    the `stable` object is the only durability signal here and it is one object.
  - **Failures are reported with their reasons, not just counted.** A count
    tells you something broke; the reason tells you whether it was the network,
    the service, or us."
  (:require [clojure.string :as str]))

(def min-window-hours
  "Below this, no rate is reported at all. 168 = one week: short enough to be
  reachable in Phase 0, long enough that a single provider incident does not
  set the estimate by itself."
  168)

(defn- parse-log [text]
  (->> (str/split-lines (or text ""))
       (remove str/blank?)
       (keep (fn [l] (try (js->clj (js/JSON.parse l) :keywordize-keys true)
                          (catch :default _ nil))))
       vec))

(defn- hours-between [a b]
  (/ (- (js/Date.parse b) (js/Date.parse a)) 3600000.0))

(defn- pct [n d] (if (zero? d) nil (* 100.0 (/ (double n) d))))

(defn summarise
  "Turn a probe log into a status report."
  [text now-iso]
  (let [rounds (parse-log text)
        n (count rounds)]
    (if (zero? n)
      {:samples 0 :note "no probe rounds recorded yet"}
      (let [first-at (:at (first rounds))
            window (hours-between first-at now-iso)
            summarise-one
            (fn [rs]
              (let [total (count rs)
                    bad (remove :ok rs)
                    read-ms (keep :read-ms (filter :ok rs))]
                {:samples total
                 :failures (count bad)
                 :availability-pct (pct (- total (count bad)) total)
                 :median-read-ms (when (seq read-ms)
                                   (nth (sort read-ms) (quot (count read-ms) 2)))
                 ;; The one durability signal in the whole probe: the stable
                 ;; object had to be rewritten, which means it was gone.
                 :stable-object-lost (count (filter :stable-rewrote rs))
                 :failure-reasons (into (sorted-map)
                                        (frequencies (keep :error bad)))}))
            per-backend
            (into (sorted-map)
                  (map (fn [[b rs]] [b (summarise-one rs)]))
                  (group-by :backend (mapcat :backends rounds)))]
        {:samples n
         :window-hours (js/Math.round window)
         :first-at first-at
         :last-at (:at (last rounds))
         :backends per-backend
         :node-loss-rate
         (if (< window min-window-hours)
           {:status :insufficient-window
            :have-hours (js/Math.round window)
            :need-hours min-window-hours
            :note (str "a rate estimated from " (js/Math.round window)
                       "h of probes would be noise wearing the clothes of a "
                       "fact. Reported at " min-window-hours "h.")}
           {:status :available
            :note (str "per-backend failures over " (js/Math.round window)
                       "h; see :backends. This is an AVAILABILITY series — "
                       "durability is the :stable-object-lost column, and it "
                       "is one object per backend.")})
         :what-this-is-not
         ["not a durability figure — availability is not durability"
          "not a nines claim — no nines are computable from this"
          "not the node-loss rate for third-party operators — these are rented backends"]}))))
