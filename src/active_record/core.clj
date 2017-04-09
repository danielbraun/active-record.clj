(ns active-record.core)

(defprotocol ActiveRecord
  (save [this])
  (save! [this])
  (destroy [this])
  (errors [this])
  (id [this])
  (new? [this])
  (valid? [this])
  (schema [this])
  (coerce [this]))
