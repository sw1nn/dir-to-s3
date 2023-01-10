(ns chunked.multipart-upload.s3-api
  (:require
   [clojure.datafy :as d])
  (:import
   (software.amazon.awssdk.awscore AwsRequestOverrideConfiguration)
   (software.amazon.awssdk.services.s3.model
     CompletedMultipartUpload
     CompletedPart
     Initiator
     MultipartUpload
     Owner
     S3ResponseMetadata
     StorageClass
     UploadPartResponse))
  )


(defn ->s3-metadata [m]
  {"xtdb-metadata" (pr-str m)})

(defn override-api-timeouts
  "Overrides the overall api call timeout (including retries)"
  [^S3Request$Builder builder config]
  (let [override-config (s3api/->aws-request-override-configuration
                         (.overrideConfiguration builder) config)]
    (.overrideConfiguration builder override-config)))

(defn ->completed-part [{:keys [e-tag part-number] :as m}]
  (-> (CompletedPart/builder)
      (.eTag e-tag)
      (.partNumber part-number)
      (.build)))

(defn ->completed-multipart-upload [part-stages]
  (-> (CompletedMultipartUpload/builder)
      (.parts (->> part-stages
                   (sort-by :part-number) ;; required by S3 API
                   (map ->completed-part)))
      (.build)))

(defn ->aws-request-override-configuration [^AwsRequestOverrideConfiguration configuration
                                            {:keys [api-call-timeout
                                                    api-call-attempt-timeout
                                                    ;; TODO handle other attrs
                                                    ]}]
  (-> (if configuration
        (.toBuilder configuration)
        (AwsRequestOverrideConfiguration/builder))
      (cond->
          api-call-timeout (.apiCallTimeout api-call-timeout)
          api-call-attempt-timeout (.apiCallAttemptTimeout api-call-attempt-timeout))
      (.build)))

(extend-protocol d/Datafiable
  MultipartUpload
  (datafy [d]
    {:upload-id (.uploadId d)
     :key (.key d)
     :owner (.owner d)
     :storage-class (.storageClass d)
     :initiator (.initiator d)})
  StorageClass
  (datafy [d]
    (str d))
  Owner
  (datafy [d]
    (.id d))
  Initiator
  (datafy [d]
    {:id (.id d)
     :display-name (.displayName d)})
  S3ResponseMetadata
  (datafy [d]
    {:request-id (.requestId d)
     :extended-request-id (.extendedRequestId d)
     :cloud-front-id (.cloudFrontId d)})
  UploadPartResponse
  (datafy [response]
    (let [m (if (-> response (.sdkHttpResponse) (.isSuccessful))
              {:e-tag (-> response (.eTag))}
              {:error (some-> (.statusText response)
                              (.orElse "Failed to Upload part, reasons unknown"))})]
      (with-meta m
        {:aws/response-metadata (-> response
                                    .responseMetadata
                                    d/datafy)})))
  AwsRequestOverrideConfiguration
  (datafy [d]
    (let [api-call-timeout (.apiCallTimeout d)
          api-call-attempt-timeout (.apiCallAttemptTimeout d)]
      (cond-> {}
        (.isPresent api-call-timeout) (assoc :api-call-timeout (.get api-call-timeout))
        (.isPresent api-call-attempt-timeout) (assoc :api-call-attempt-timeout (.get api-call-attempt-timeout))))))
