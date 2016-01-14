(defproject redshiftimport "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :main redshiftimport.core
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [sjdbc "0.1.5"]
                 [org.clojure/data.json "0.2.6"]
                 [com.amazonaws/aws-java-sdk "1.10.41"]
                 [local/RedshiftJDBC41 "1.1.10.1010"]
                 [org.apache.hadoop/hadoop-hdfs "2.6.0-cdh5.4.5"]
                 [org.apache.hadoop/hadoop-common "2.6.0-cdh5.4.5" :exclusions [org.apache.httpcomponents/httpcore]]
                 ;[org.clojure/tools.nrepl "0.2.11"]
                 [org.clojure/tools.cli "0.3.3"]]

  :repositories [ ["cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos"] ]

  :profiles {
             :uberjar {:aot :all}
             })
