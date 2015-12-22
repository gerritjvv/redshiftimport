# redshiftimport

Load data from HDFS via S3 into Redshift

## Usage

```clojure

java -jar ./redshiftimport-0.1.0-SNAPSHOT-standalone.jar \
--redshift-url "jdbc:redshift://<myserver>:5439/<mydb>" \
--redshift-user <awsuser> \
--redshift-pwd '<pwd>' \
--redshift-table <table> \
--s3-access <key> \
--s3-secret <secret> \
--s3-region eu-central-1 \
--s3-bucket /<bucket> \
--s3-path <mydir/anotherdir> \
--threads 8 \
--hdfs-url hdfs://<namenode> \
--hdfs-path /mypathinhdfs/*

```

### Repeatable S3 Loads

The HDFS directory can be used to create a unique file name for the s3 upload into the same bucket.  
This allows us to re-run s3 uploads without creating duplicate s3 files.

Specify the ```hdfs-s3-prefix-depth``` option.


### Only S3 Loading (disable redshift)

use the ```--disable-redshift``` flag to only load to s3.  
Note that the manifest files will not be loaded either.  

## Trouble Shooting

If the redshift copy hangs see: http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-firewall-guidance.html


## License

Copyright © 2015 gerritjvv

Distributed under the Eclipse Public License either version 1.0
