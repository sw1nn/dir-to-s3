(ns dir-to-s3
  (:require
   [archive.directory :as dir]
   [clojure.datafy :as d]
   [chunked.output-stream :as c]
   [chunked.multipart-upload :as mpu ])
    (:import
     (java.nio.file Paths)))

(defn dir-to-s3 [dir s3 metadata]
  (let [os (c/output-stream (mpu/create-buffer-sink s3 metadata))]
    (dir/archive-directory dir dir/simple-path-pred os )))

(comment
  (def s3-client (-> (software.amazon.awssdk.services.s3.S3AsyncClient/builder)
                     (.build)))

  (def test-dir "test-dir")
  (def test-bucket-name "a-test-bucket") ;; should exist

  (def s3  {:client s3-client
            :bucket test-bucket-name
            :key "foo/bar/baz.tgz"
            :part-upload-timeout (java.time.Duration/parse "PT10M")})

  (def  metadata {:xtdb.checkpoint/cp-format {:index-version 20,
                                              :xtdb.rocksdb/version "6"},
                  :tx {:xtdb.api/tx-time #inst "2022-07-18T13:32:12.793-00:00",
                       :xtdb.api/tx-id 9},
                  :xtdb.checkpoint/checkpoint-at #inst "2022-07-21T10:05:01.908-00:00"})

  (dir-to-s3 (Paths/get test-dir  (make-array String 0)) s3 metadata )

  ;; tidy up failed uploads - if you don't do this you have space
  ;; allocated in S3 that is not 'obvious' and you still have to pay
  ;; for.
  (doseq [upload-id (->> s3 mpu/list-multipart-uploads (map (comp :upload-id d/datafy)))]
    (.println *err* (str "aborting " upload-id))
    (mpu/abort-multipart-upload s3 upload-id))
  )
