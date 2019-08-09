#/bin/bash

PRODUCT='Couchbase CLI'
AUTHOR=Couchbase
VERSION='1.0.0'

function gen_docs {
  infile=$1
  man_volnum=$2
  html_folder="$(echo $1 | cut -d '-' -f 1,2 | cut -d '.' -f 1)"
  ext="${infile##*.}"
  name="${infile%.*}"

  # Generate HTML
  mkdir -p generated/doc/${html_folder}
  asciidoctor -b html \
    -d manpage \
    -a mansource="$PRODUCT $VERSION" \
    -a manmanual="$PRODUCT Manual" \
    -a author="$AUTHOR" \
    -a partialsdir=../_partials \
    -a nofooter \
    -r ./asciidoctor-ext.rb \
    -o generated/doc/${html_folder}/${name}.html \
    modules/cli/pages/cbcli/${infile}

  # Generate man pages
  mkdir -p generated/man/man${man_volnum}
  asciidoctor -b manpage \
    -d manpage \
    -a mansource="$PRODUCT $VERSION" \
    -a manmanual="$PRODUCT Manual" \
    -a author="$AUTHOR" \
    -a partialsdir=../_partials \
    -r ./asciidoctor-ext.rb \
    -D generated/man/man${man_volnum} \
    modules/cli/pages/cbcli/${infile}
}

gen_docs couchbase-cli-bucket-compact.adoc 1
gen_docs couchbase-cli-bucket-create.adoc 1
gen_docs couchbase-cli-bucket-delete.adoc 1
gen_docs couchbase-cli-bucket-edit.adoc 1
gen_docs couchbase-cli-bucket-flush.adoc 1
gen_docs couchbase-cli-bucket-list.adoc 1
gen_docs couchbase-cli-cluster-edit.adoc 1
gen_docs couchbase-cli-node-to-node-encryption.adoc 1
gen_docs couchbase-cli-cluster-init.adoc 1
gen_docs couchbase-cli-ip-family.adoc 1
gen_docs couchbase-cli-collect-logs-start.adoc 1
gen_docs couchbase-cli-collect-logs-status.adoc 1
gen_docs couchbase-cli-collect-logs-stop.adoc 1
gen_docs couchbase-cli-collection-manage.adoc 1
gen_docs couchbase-cli-eventing-function-setup.adoc 1
gen_docs couchbase-cli-failover.adoc 1
gen_docs couchbase-cli-group-manage.adoc 1
gen_docs couchbase-cli-help.adoc 1
gen_docs couchbase-cli-host-list.adoc 1
gen_docs couchbase-cli-node-init.adoc 1
gen_docs couchbase-cli-master-password.adoc 1
gen_docs couchbase-cli-rebalance.adoc 1
gen_docs couchbase-cli-rebalance-status.adoc 1
gen_docs couchbase-cli-rebalance-stop.adoc 1
gen_docs couchbase-cli-recovery.adoc 1
gen_docs couchbase-cli-reset-admin-password.adoc 1
gen_docs couchbase-cli-reset-cipher-suites.adoc 1
gen_docs couchbase-cli-server-add.adoc 1
gen_docs couchbase-cli-server-eshell.adoc 1
gen_docs couchbase-cli-server-info.adoc 1
gen_docs couchbase-cli-server-list.adoc 1
gen_docs couchbase-cli-server-readd.adoc 1
gen_docs couchbase-cli-setting-alert.adoc 1
gen_docs couchbase-cli-setting-audit.adoc 1
gen_docs couchbase-cli-setting-autofailover.adoc 1
gen_docs couchbase-cli-setting-autoreprovision.adoc 1
gen_docs couchbase-cli-setting-cluster.adoc 1
gen_docs couchbase-cli-setting-compaction.adoc 1
gen_docs couchbase-cli-setting-index.adoc 1
gen_docs couchbase-cli-setting-saslauthd.adoc 1
gen_docs couchbase-cli-setting-ldap.adoc 1
gen_docs couchbase-cli-setting-query.adoc 1
gen_docs couchbase-cli-setting-notification.adoc 1
gen_docs couchbase-cli-setting-password-policy.adoc 1
gen_docs couchbase-cli-setting-security.adoc 1
gen_docs couchbase-cli-setting-rebalance.adoc 1
gen_docs couchbase-cli-setting-xdcr.adoc 1
gen_docs couchbase-cli-ssl-manage.adoc 1
gen_docs couchbase-cli-user-change-password.adoc 1
gen_docs couchbase-cli-user-manage.adoc 1
gen_docs couchbase-cli-xdcr-replicate.adoc 1
gen_docs couchbase-cli-xdcr-setup.adoc 1
gen_docs couchbase-cli.adoc 1
gen_docs cblogredaction.adoc 1
gen_docs couchbase-cli-enable-developer-preview.adoc 1
gen_docs couchbase-cli-setting-alternate-address.adoc 1
gen_docs couchbase-cli-setting-on-demand.adoc 1
