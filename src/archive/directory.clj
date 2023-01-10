(ns archive.directory
  (:require [clojure.java.io :as io])
  (:import (org.apache.commons.compress.archivers.tar TarArchiveOutputStream)
           (org.apache.commons.compress.compressors.gzip GzipCompressorOutputStream)
           (java.nio.file Files Path FileVisitOption LinkOption OpenOption)))

(defn add-entry! [os path entry-name]
  (let [entry (.createArchiveEntry os path entry-name (make-array LinkOption 0))]
      (.putArchiveEntry os entry)
      (with-open [is (Files/newInputStream path (make-array OpenOption 0))]
        (io/copy is os))
      (.closeArchiveEntry os)))

(defn archive-directory [^Path dir path-pred dest-os]
  (with-open [tgz (-> dest-os
                      GzipCompressorOutputStream.
                      TarArchiveOutputStream.)]
    (doseq [^Path path (-> (Files/walk dir Integer/MAX_VALUE (make-array FileVisitOption 0))
                           .iterator
                           iterator-seq)
            :when (and (path-pred path)
                       (-> path .toFile .isFile))
            :let [entry-name (str (.relativize dir path))]]
      (add-entry! tgz path entry-name))))

(defn simple-path-pred [^Path p]
  (not (-> p .toFile .isHidden)))
