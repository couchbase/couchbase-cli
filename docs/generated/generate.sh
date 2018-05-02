#/bin/bash

function gen_docs {
  infile=$1
  man_folder=$2
  html_folder="$(echo $1 | cut -d '-' -f 1,2 | cut -d '.' -f 1)"
  ext="${infile##*.}"
  name="${infile%.*}"

  # Generate html
  asciidoc -b xhtml11 -d manpage --conf-file=../asciidoc.conf -o doc/${html_folder}/${name}.html ../${infile}

  # Generate man
  asciidoc -b docbook -d manpage --conf-file=../asciidoc.conf -o tmp.xml ../${infile}
  XML_CATALOG_FILES=/usr/local/etc/xml/catalog xmlto -o man/man${man_folder} man tmp.xml
  rm tmp.xml
}

mkdir -p ./doc/couchbase-cli

gen_docs couchbase-cli-admin-role-manage.txt 1
gen_docs couchbase-cli-bucket-compact.txt 1
gen_docs couchbase-cli-bucket-create.txt 1
gen_docs couchbase-cli-bucket-delete.txt 1
gen_docs couchbase-cli-bucket-edit.txt 1
gen_docs couchbase-cli-bucket-flush.txt 1
gen_docs couchbase-cli-bucket-list.txt 1
gen_docs couchbase-cli-cluster-edit.txt 1
gen_docs couchbase-cli-cluster-init.txt 1
gen_docs couchbase-cli-collect-logs-start.txt 1
gen_docs couchbase-cli-collect-logs-status.txt 1
gen_docs couchbase-cli-collect-logs-stop.txt 1
gen_docs couchbase-cli-eventing-function-setup.txt 1
gen_docs couchbase-cli-failover.txt 1
gen_docs couchbase-cli-group-manage.txt 1
gen_docs couchbase-cli-help.txt 1
gen_docs couchbase-cli-host-list.txt 1
gen_docs couchbase-cli-master-password.txt 1
gen_docs couchbase-cli-node-init.txt 1
gen_docs couchbase-cli-rebalance-status.txt 1
gen_docs couchbase-cli-rebalance-stop.txt 1
gen_docs couchbase-cli-rebalance.txt 1
gen_docs couchbase-cli-recovery.txt 1
gen_docs couchbase-cli-reset-admin-password.txt 1
gen_docs couchbase-cli-server-add.txt 1
gen_docs couchbase-cli-server-eshell.txt 1
gen_docs couchbase-cli-server-info.txt 1
gen_docs couchbase-cli-server-list.txt 1
gen_docs couchbase-cli-server-readd.txt 1
gen_docs couchbase-cli-setting-alert.txt 1
gen_docs couchbase-cli-setting-audit.txt 1
gen_docs couchbase-cli-setting-autofailover.txt 1
gen_docs couchbase-cli-setting-autoreprovision.txt 1
gen_docs couchbase-cli-setting-cluster.txt 1
gen_docs couchbase-cli-setting-compaction.txt 1
gen_docs couchbase-cli-setting-index.txt 1
gen_docs couchbase-cli-setting-ldap.txt 1
gen_docs couchbase-cli-setting-master-password.txt 1
gen_docs couchbase-cli-setting-notification.txt 1
gen_docs couchbase-cli-setting-password-policy.txt 1
gen_docs couchbase-cli-setting-security.txt 1
gen_docs couchbase-cli-setting-xdcr.txt 1
gen_docs couchbase-cli-ssl-manage.txt 1
gen_docs couchbase-cli-user-manage.txt 1
gen_docs couchbase-cli-xdcr-replicate.txt 1
gen_docs couchbase-cli-xdcr-setup.txt 1
gen_docs couchbase-cli.txt 1
