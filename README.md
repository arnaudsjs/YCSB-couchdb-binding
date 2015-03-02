YCSB-couchdb-binding
====================

Couchdb database interface for YCSB

Installation guide
==================

* Download the YCSB project as follows: git clone https://github.com/brianfrankcooper/YCSB.git
* Include the YCSB binding within the YCSB directory: git clone https://github.com/arnaudsjs/YCSB-couchdb-binding.git couchdb
* Add <module>couchdb</module> to the list of modules in YCSB/pom.xml
* Add the following lines to the DATABASE section in YCSB/bin/ycsb: "couchdb" : "couchdb.CouchdbClient"
* compile everything by executing the following command within the YCSB directory: mvn clean package
