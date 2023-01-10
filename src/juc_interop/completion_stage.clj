(ns juc-interop.completion-stage)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (incomplete) various JDK8 java.util.function.CompletionStage
;; helpers

(defn then-apply
  "When the given completion stage finishes successfully, call f with
  the result of the successful stage and return the result.

  In the case of an exception from cstage, f is NOT called
  and the exception propagates."
  [cstage f]
  (.thenApply cstage
              (reify java.util.function.Function
                (apply [_this res]
                  (f res)))))

(defn then-run
  [cstage f]
  (.thenRun cstage f))

(defn exceptionally
  [cstage f]
  (.exceptionally cstage
   (reify java.util.function.Function
     (apply [_this ex]
       (f ex)))))

(defn handle
  "When the given completion stage finishes, call f with the result (or
  nil if none) and exception (or nil if none) of that stage and return
  the result."
  [cstage f]
  (.handle cstage
           (reify java.util.function.BiFunction
             (apply [_this res ex]
               (f res ex)))))

(defn when-complete
  "When the given completion stage finishes, call f "
  [cstage f]
  (.whenComplete cstage
                 (reify java.util.function.BiConsumer
                   (accept [_this res ex]
                     (f res ex)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
