#!/bin/bash
mkdir -p html
mkdir -p ../man/man1
for filename in couchbase-cli*; do
        echo $filename
		name="${filename%.*}"
		asciidoc -o html/${name}.html $filename
		asciidoc -o ../man/${name}.xml -b docbook -d manpage -f asciidoc.conf $filename
		xmlto man ../man/${name}.xml -o ../man/man1/
		rm ../man/${name}.xml
done

#asciidoc -b docbook -d manpage -f asciidoc.conf couchbase-cli-cluster-edit.txt
#xmlto man couchbase-cli-cluster-edit.xml
#rm couchbase-cli-cluster-edit.xml
