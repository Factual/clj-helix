(ns clj-helix.logging
  "Take a big step back, and literally FUCK YOUR OWN FACE.")

(defn all-log4j-loggers []
  (->> (org.apache.log4j.LogManager/getCurrentLoggers)
       (java.util.Collections/list)
       (cons (org.apache.log4j.LogManager/getRootLogger))))

(defn all-jdk-loggers []
  (let [manager (java.util.logging.LogManager/getLogManager)]
    (->> manager
         .getLoggerNames
         java.util.Collections/list
         (map #(.getLogger manager %)))))

(defmacro mute-jdk [& body]
  `(let [loggers# (all-jdk-loggers)
         levels#  (map #(.getLevel %) loggers#)]
     (try
       (doseq [l# loggers#]
         (.setLevel l# java.util.logging.Level/OFF))
       ~@body
       (finally
         (dorun (map (fn [logger# level#] (.setLevel logger# level#))
                     loggers#
                     levels#))))))

(defmacro mute-log4j [& body]
  `(let [loggers# (all-log4j-loggers)
         levels#  (map #(.getLevel %) loggers#)]
     (try
       (doseq [l# loggers#]
         (.setLevel l# org.apache.log4j.Level/OFF))
       ~@body
       (finally
         (dorun (map (fn [logger# level#] (.setLevel logger# level#))
                     loggers#
                     levels#))))))

(defmacro mute [& body]
  `(mute-jdk
     (mute-log4j
       ~@body)))
