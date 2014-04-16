(ns jepsen.system.consul-test
  (:use jepsen.system.consul
        jepsen.core
        jepsen.core-test
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.util      :as util]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]
            [jepsen.report    :as report]))

(deftest register-test
  (let [test (run!
               (assoc
                 noop-test
                 :name      "consul"
                 :os        debian/os
                 :db        (db)
                 :client    (cas-client)
                 :model     (model/cas-register)
                 :ssh       {:username "root"
                             :private-key-path "~/.ssh/id_rsa"
                             :strict-host-key-checking false
                             }
                 :checker   (checker/compose {:html   timeline/html
                                              :linear checker/linearizable})
                 :nemesis   (nemesis/partition-random-halves)
                 :generator (gen/phases
                              (->> gen/cas
                                   (gen/delay 1/2)
                                   (gen/nemesis
                                     (gen/seq
                                       (cycle [(gen/sleep 5)
                                               {:type :info :f :start}
                                               (gen/sleep 5)
                                               {:type :info :f :stop}])))
                                   (gen/time-limit 20))
                              (gen/nemesis
                                (gen/once {:type :info :f :stop}))
;                              (gen/sleep 10)
                              (gen/clients
                                (gen/once {:type :invoke :f :read})))))]
    (is (:valid? (:results test)))
    (report/linearizability (:linear (:results test)))))
