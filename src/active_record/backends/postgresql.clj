(ns active-record.backends.postgresql
  (:require [active-record.core :as ar]
            [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [schema.core :as s]
            [schema.coerce :as coerce]
            [schema.utils :refer [error?]])
  (:import [org.joda.time Interval]
           [java.lang Short]
           [javax.mail.internet InternetAddress]))

(defn email-valid? [email]
  ;TODO return detailed errors in exception as map
  (try (.validate (InternetAddress. email))
       true
       (catch javax.mail.internet.AddressException e false)))

(defn table-primary-key [db table-name]
  (some->>
    (jdbc/query db
                ["SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
                 FROM   pg_index i
                 JOIN   pg_attribute a ON a.attrelid = i.indrelid
                 AND a.attnum = ANY (i.indkey)
                 WHERE  i.indrelid = ?::regclass
                 AND    i.indisprimary; "
                 (name table-name)])
    first
    :attname
    keyword))

(defmulti column->schema :udt_name)

(defmethod column->schema "bool" [_] s/Bool)
(defmethod column->schema "char" [_] s/Str)
(defmethod column->schema "bpchar" [_] s/Str)
(defmethod column->schema "float4" [_] Float)
(defmethod column->schema "float8" [_] Double)
(defmethod column->schema "inet" [_] java.net.InetAddress)
(defmethod column->schema "int2" [_] java.lang.Short)
(defmethod column->schema "int4" [_] s/Int)
(defmethod column->schema "int8" [_] Long)
(defmethod column->schema "interval" [_] Interval)
(defmethod column->schema "numeric" [_] s/Num)
(defmethod column->schema "text" [_] s/Str)
(defmethod column->schema "timestamptz" [_] s/Inst)
(defmethod column->schema "varchar" [_] s/Str)

(defn max-length [n]
  (fn [s]
    (<= (count (str s)) n)))

(defn table-schema [db table]
  {:post [(not-empty %)]}
  (->> (sql/format {:select [:*]
                    :from [:information_schema.columns]
                    :where [:= :table_name (name table)]})
       (jdbc/query db)
       (map (fn [{:keys [column_name
                         character_maximum_length
                         column_default
                         data_type
                         is_nullable] :as column}]
              [(cond-> (keyword column_name)
                 (or (#{"YES"} is_nullable) column_default) s/optional-key)
               (cond-> (column->schema column)
                 (re-find #"email" column_name) (s/constrained email-valid?)
                 character_maximum_length (s/constrained (max-length character_maximum_length)
                                                         'string-too-long)
                 (#{"YES"} is_nullable) s/maybe)]))
       (into {})))

(defn base [db table]
  (let [pk-clause (fn [this]
                    [:= (table-primary-key db table) (#'ar/id this)])
        coerce (fn [datum]
                 ((coerce/coercer (table-schema db table)
                                  (some-fn coerce/string-coercion-matcher
                                           {s/Str str})) datum))]
    {:id (fn [this]
           (get this (table-primary-key db table)))
     :new? (comp nil? #'ar/id)
     :save (fn [this]
             (when (#'ar/valid? this)
               (if (#'ar/new? this)
                 (->> this
                      coerce
                      (jdbc/insert! db table)
                      first
                      (merge this))
                 (do
                   (jdbc/execute! db
                                  (sql/format {:update table
                                               :sset (coerce this)
                                               :where (pk-clause this)}))
                   (->> (sql/format {:select [:*]
                                     :from table
                                     :where (pk-clause this)})
                        (jdbc/execute! db)
                        (merge this))))))
     :errors (fn [this]
               (let [coerced (coerce this)]
                 (when (error? coerced)
                   (:error coerced))))
     :valid? (comp empty? #'ar/errors)
     :destroy (fn [this]
                (assert (not (ar/new? this)))
                (->> (sql/format {:delete-from table
                                  :where (pk-clause this)})
                     (jdbc/execute! db))
                nil)
     :schema (fn [_] (table-schema db table))}))
