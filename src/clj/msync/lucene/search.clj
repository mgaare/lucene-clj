(ns msync.lucene.search
  (:require [msync.lucene.query :as query])
  (:import [org.apache.lucene.search ScoreDoc TopDocs Query IndexSearcher]
           [org.apache.lucene.index IndexReader IndexableField]
           [org.apache.lucene.search.highlight Highlighter TokenSources
            QueryScorer TextFragment]))


(defn search [^IndexReader index-store query-form
              {:keys [field-name results-per-page analyzer hit->doc page fuzzy?]
               :or   {results-per-page 10
                      page             0
                      hit->doc         identity
                      fuzzy?           false}}]
  (let [^IndexSearcher searcher (IndexSearcher. index-store)
        field-name              (if field-name (name field-name))
        ^Query query            (if fuzzy?
                                  (query/combine-fuzzy-queries query-form)
                                  (query/parse query-form {:analyzer analyzer :field-name field-name}))
        ^TopDocs hits           (.search searcher query (int (+ (* page results-per-page) results-per-page)))
        start                   (* page results-per-page)
        end                     (min (+ start results-per-page) (.value (.totalHits hits)))]
    (vec
     (for [^ScoreDoc hit (map (partial aget (.scoreDocs hits))
                              (range start end))]
       (let [doc-id (.doc hit)
             doc    (.doc searcher doc-id)
             score  (.score hit)]
         {:doc-id doc-id :score score :hit (hit->doc doc)})))))

(defn search-with-highlights
  "A search that uses a highlighter to get relevant offsets."
  [^IndexReader index-store query-form
   {:keys [field-name results-per-page analyzer hit->doc page fuzzy?]
    :or   {results-per-page 10
           page             0
           hit->doc         identity
           fuzzy?           false}}]
  (let [^IndexSearcher searcher (IndexSearcher. index-store)
        field-name              (if field-name (name field-name))
        ^Query query            (if fuzzy?
                                  (query/combine-fuzzy-queries query-form)
                                  (query/parse query-form {:analyzer analyzer :field-name field-name}))
        ^Highlighter highlighter (Highlighter. (QueryScorer. query))
        ^TopDocs hits           (.search searcher query (int (+ (* page results-per-page) results-per-page)))
        start                   (* page results-per-page)
        end                     (min (+ start results-per-page) (.value (.totalHits hits)))
        ]
    (vec
     (for [^ScoreDoc hit (map (partial aget (.scoreDocs hits))
                              (range start end))]
       (let [doc-id (.doc hit)
             doc    (.doc searcher doc-id)
             score  (.score hit)
             frags  (let [^TokenSources token-stream
                          (TokenSources/getAnyTokenStream
                           (.getIndexReader searcher)
                           doc-id field-name analyzer)
                          text (.get doc field-name)]
                      ;; lucene barfs up some exceptions here, so wrapping in try catch
                      (try
                        (->> (.getBestTextFragments highlighter token-stream text true 10)
                             (map (fn [tf]
                                    {:highlighted (.toString ^TextFragment tf)
                                     :score (.getScore ^TextFragment tf)}))
                             (into []))
                        (catch Exception e [])))]
         {:doc-id doc-id
          :score score
          :hit (hit->doc doc)
          :fragments frags})))))
