(ns chunked.output-stream
  (:require [juc-interop.completion-stage :as cs])
  (:import
   (com.sw1nn.chunked ChunkedOutputStream ChunkedOutputStreamDelegate)
   (java.io Flushable IOException)
   (java.lang AutoCloseable)
   (java.nio ByteBuffer)
   (java.util.concurrent ArrayBlockingQueue BlockingQueue)))

;; TODO: defaults require tuning - howto come up with reasonable values here?
;; NOTE - AWS minimum size is 5Mb for Multipart upload, so this should be higher than that.
(def default-buffer-capacity (* 8 1024 1024) )
(def default-buffer-count 3)

;; ByteBuffer TL;DR
;;
;;  We strictly only append bytes to ByteBuffer, using bulk .put
;;
;;   * position  - index where next byte will be .put or .get
;;   * limit     - index of first byte that should not be read/written
;;   * remaining - bytes remaining before buffer reaches capacity
;;   * capacity  - size of buffer
;;
;;   .flip       - set limit=position, position=0

(defprotocol BufferSink
  (write-async! [this buffer metadata]))


(defn wrapped-take [queue]
  (try
    (.take queue)
    (catch InterruptedException e
      (throw (IOException. e)))))

(defn- write-buffer+recycle!
  "Write the buffer to the sink.
  Returns a buffer that is ready for further writes (could be the same
  buffer after .clear, but doesn't need to be)
  "
  [queue buffer-index output]
  (let [buffer (wrapped-take queue)]
    (-> output
        (write-async! buffer {:index buffer-index})
        (cs/when-complete
         (fn [_res _ex]
           (->> buffer
                (.clear)
                (.put queue))))
        (deref))))


(deftype BlockingQueueOutputStreamDelegate [^BlockingQueue queue
                                            buffer-index-fn
                                            output]
  ChunkedOutputStreamDelegate
  (writeInt [this b]
    (let [buf (.peek queue)]
      (.put buf b)
      (when (zero? (.remaining buf))
        (write-buffer+recycle! queue (buffer-index-fn) output))))

  (writeArraySlice [_this b offset len]
    (when (or (neg? offset)
              (neg? len)
              (> (+ offset len) (count b)))
      (throw (ArrayIndexOutOfBoundsException.)))

    (loop [offset offset
           len len]
      (let [buf (.peek queue)
            remaining (.remaining buf)]
        (when (zero? remaining)
          (write-buffer+recycle! queue (buffer-index-fn) output))

        (let [excess? (> len remaining)]
          (.put buf b offset (if excess? remaining len))
          (when excess?
            (recur (+ offset remaining)
                   (- len remaining)))))))

  (writeArray [this b]
    (.writeArraySlice this b 0 (count b)))

  Flushable
  (flush [_]
    ;; (Optional) flushing not implemented,
    ;; could flush if buffer is, say, 50% full or similar?
    )

  AutoCloseable
  (close [_]
    (write-async! output (wrapped-take queue) {:index (buffer-index-fn)})
    (.close output)

    ;; drain queue, buffers will eventually get GC'd
    (while (seq queue)
      (.remove queue))))

(defn- output-stream-delegate [queue output]
  (let [index (atom 0)]
    (->BlockingQueueOutputStreamDelegate queue
                                         (fn buffer-index-fn [] (swap! index (comp int inc)))
                                         output)))

(defn filled-buffer-queue
  "
  Returns a BlockingQueue<ByteBuffer> of size `queue-size` filled with
  buffers of capacity `buffer-capacity`
  "
  [buffer-capacity queue-size]
  (let [fairness false]
    (->> (repeatedly #(ByteBuffer/allocateDirect buffer-capacity))
         (take queue-size)
         (ArrayBlockingQueue. queue-size fairness))))

(defn output-stream
  "
  Returns a chunking output-stream. Calls to the various write(b ...)
  methods are stored in intermediate buffers. When those buffers are
  full they are written asynchronously to the specified `BufferSink`
  "
  ([sink {:keys [buffer-capacity buffer-count]
            :or {buffer-capacity default-buffer-capacity
                 buffer-count default-buffer-count}}]

   (ChunkedOutputStream. (output-stream-delegate
                          (filled-buffer-queue buffer-capacity buffer-count)
                          sink)))
  ([sink]
   (output-stream sink nil)))
