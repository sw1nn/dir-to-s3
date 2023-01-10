(ns chunked.multipart-upload
  (:require
   [chunked.output-stream :as chunked]
   [chunked.multipart-upload.s3-api :as s3api]
   [clojure.datafy :as d]
   [clojure.java.io :as io]
   [clojure.tools.logging :as log]
   [juc-interop.completion-stage :as cs])
  (:import
   (java.lang AutoCloseable)
   (java.util.concurrent CompletableFuture)
   (software.amazon.awssdk.core.async AsyncRequestBody)
   (software.amazon.awssdk.services.s3 S3AsyncClient)
   (software.amazon.awssdk.services.s3.model
     AbortMultipartUploadRequest
     CompleteMultipartUploadRequest
     CreateMultipartUploadRequest
     ListMultipartUploadsRequest
     S3Request$Builder
     UploadPartRequest)))

(defn list-multipart-uploads [s3]
  (let [{:keys [^S3AsyncClient client bucket]} s3
        request (-> (ListMultipartUploadsRequest/builder)
                    (.bucket bucket)
                    (.build))]
    (-> client
        (.listMultipartUploads request)
        (deref)
        (.uploads)
        (->> (map d/datafy)))))

(defn create-multipart-upload [{:keys [^S3AsyncClient client bucket key] :as s3} metadata]
  (let [request (-> (CreateMultipartUploadRequest/builder)
                    (.bucket bucket)
                    (.key key)
                    (.metadata (s3api/->s3-metadata metadata))
                    (.build))]
    (-> client
        (.createMultipartUpload request)
        (deref)
        (.uploadId))))

(defn abort-multipart-upload [{:keys [^S3AsyncClient client bucket key] :as s3} upload-id]
;; https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3AsyncClient.html#abortMultipartUpload-software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest-

  (let [request (-> (AbortMultipartUploadRequest/builder)
                    (.bucket bucket)
                    (.key key)
                    (.uploadId upload-id)
                    (.build))]
    (-> client
        (.abortMultipartUpload request)
        (cs/then-run #(log/infof "Aborted Multipart upload id='%s'" upload-id ))
        (deref))))

(defn complete-multipart-upload [{:keys [^S3AsyncClient client bucket key]} upload-id parts]
  (let [multipart-upload (s3api/->completed-multipart-upload parts)
        request (-> (CompleteMultipartUploadRequest/builder)
                    (.bucket bucket)
                    (.key key)
                    (.uploadId upload-id)
                    (.multipartUpload multipart-upload)
                    (.build))]
    (-> client
        (.completeMultipartUpload request)
        (cs/then-run #(log/infof "Completed Multipart upload, id='%s'" upload-id ))
        (deref))))

(defn confirm-no-outstanding-parts [s3 upload-id]
  ;; any uploadPart requests that were in progress when an
  ;; abortMultipartUpload request was made, may have completed since,
  ;; so it's recommended that checks are made and abort called
  ;; multiple times if necessary :-/
  (loop [retry 2]
    (Thread/sleep (* 60 1000)) ;; might as well wait here
    (if-not
     (->> s3 list-multipart-uploads
          (map (comp :upload-id d/datafy))
          (filter #(= % upload-id))
          (seq))
      (log/infof "No outstanding multipart upload with id='%s' found" upload-id)
      (do
        (log/infof "Found outstanding multipart upload, id='%s', attempting to abort, retries remaining %02d" upload-id retry)
        (abort-multipart-upload s3 upload-id)))
    (when (pos? retry)
      (recur (dec retry)))))


(defrecord S3MultipartUploadSink [s3
                                  upload-id
                                  part-stages]
  chunked/BufferSink
  (write-async! [_ buf metadata]
    ;; Make calls to S3API/uploadPart asynchronously. When the call
    ;; completes, the next completion stage (then-apply) stores some
    ;; response derived data.
    ;; The CompletableFuture for those calls are then stored so that
    ;; we can wait until all uploads have completed (see close)
    (let [{:keys [^S3AsyncClient client bucket key part-upload-timeout]} s3
          part-number (:index metadata)
          request (-> (UploadPartRequest/builder)
                      (.bucket bucket)
                      (.key key)
                      (.uploadId upload-id)
                      (.partNumber part-number)
                      (cond-> part-upload-timeout (s3api/override-api-timeouts {:api-call-timeout part-upload-timeout
                                                                                :api-call-attempt-timeout (java.time.Duration/parse "PT20M")}))
                      (.build))]
      (log/debugf "Uploading part, id='%s', part-number=%05d" upload-id part-number)
      (let [uploaded-part
            (-> client
                (.uploadPart request
                             (-> buf
                                 .flip
                                 AsyncRequestBody/fromByteBuffer ;; copies
                                 ))
                (cs/then-apply (fn [result]
                                 (-> result
                                     (d/datafy)
                                     (assoc :part-number part-number)))))]
        (swap! part-stages conj uploaded-part)
        uploaded-part)))
  AutoCloseable
  (close [_]
    ;; part-stages is a collection of CompletableFutures. We wait here
    ;; to ensure that all uploads have finished before completing the
    ;; multipart upload, either calling the complete or abort api as
    ;; appropriate
    (log/infof "Waiting for %05d part uploads to complete..." (count @part-stages))
    (doto (-> (into-array CompletableFuture @part-stages)
              CompletableFuture/allOf)
        (cs/then-run #(->> @part-stages
                           (map deref)
                           (complete-multipart-upload s3 upload-id)))
        (cs/exceptionally (fn [ex]
                              (log/error "part uploading completed with error" ex)
                              (abort-multipart-upload s3 upload-id)
                              (confirm-no-outstanding-parts s3 upload-id)))
        (deref) ;; required to propagate exceptions
      )))

(defn create-buffer-sink [{:keys [^S3AsyncClient client bucket key] :as s3} metadata]
  (let [upload-id (create-multipart-upload s3 metadata)
        part-stages (atom nil)]
    (log/infof "Initiated Multipart Upload to s3://%s/%s id='%s'" bucket key upload-id)
    (->S3MultipartUploadSink s3
                             upload-id
                             part-stages)))

(comment

  (def s3-client (-> (S3AsyncClient/builder)
                     (.build)))

  (def s3 {:client s3-client
           :bucket "multi-upload-test"
           :key "foo/bar/baz.tgz"})

  (def  metadata {:xtdb.checkpoint/cp-format {:index-version 20,
                                              :xtdb.rocksdb/version "6"},
                  :tx {:xtdb.api/tx-time #inst "2022-07-18T13:32:12.793-00:00",
                       :xtdb.api/tx-id 9},
                  :xtdb.checkpoint/checkpoint-at #inst "2022-07-21T10:05:01.908-00:00"})

  (with-open [os (chunked/output-stream (create-buffer-sink s3 metadata))
              is (io/input-stream "./test.tgz")]
    (io/copy is os))

  (doseq [upload-id (->> s3 list-multipart-uploads (map (comp :upload-id d/datafy)))]
    (.println *err* (str "aborting " upload-id))
    (abort-multipart-upload s3 upload-id))

  )
