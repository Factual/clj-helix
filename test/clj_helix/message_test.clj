(ns clj-helix.message-test
  (:require [clojure.test :refer :all]
            [clj-helix.logging :refer [mute]]
            [clj-helix.fsm :refer [fsm]]
            [clj-helix.admin-test :refer [fsm-def]]
            [clj-helix.manager :refer :all]
            [clj-helix.message :refer :all]))

(use-fixtures :once #(mute (%)))

