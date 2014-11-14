TMP_DIR = ./tmp
TMP_VER = $(TMP_DIR)/version_num.tmp

default:

pythonlibdir=$(libdir)/python
nobase_pythonlib_DATA= \
         buckets.py \
         cb_bin_client.py \
         cbcollections.py \
         cbqueue.py \
         cbsnappy.py \
         cluster_stats.py \
         collector.py \
         couchbaseConstants.py \
         diskqueue_stats.py \
         info.py \
         listservers.py \
         node.py \
         node_stats.py \
         processor.py \
         pump.py \
         pump_bfd.py \
         pump_bson.py \
         pump_cb.py \
         pump_csv.py \
         pump_gen.py \
         pump_json.py \
         pump_mbf.py \
         pump_mc.py \
         pump_sfd.py \
         pump_tap.py \
         pump_transfer.py \
         pump_dcp.py \
         restclient.py \
         simplejson/LICENSE.txt \
         simplejson/__init__.py \
         simplejson/decoder.py \
         simplejson/encoder.py \
         simplejson/scanner.py \
         stats_buffer.py \
         timeout.py \
         usage.py \
         util_cli.py \
         xdcr.py


pythonlib_SCRIPTS= couchbase-cli \
                   cbbackup \
                   cbrestore \
                   cbtransfer \
                   cbclusterstats \
                   cbworkloadgen \
                   cbrecovery \
                   cbbackupwrapper \
                   cbrestorewrapper

PYTHON_TOOLS= \
        wrapper/couchbase-cli \
        wrapper/cbbackup \
        wrapper/cbrestore \
        wrapper/cbtransfer \
        wrapper/cbclusterstats \
        wrapper/cbworkloadgen \
        wrapper/cbrecovery \
        wrapper/cbbackupwrapper \
        wrapper/cbrestorewrapper

${PYTHON_TOOLS}: wrapper/wrapper
	cp $< $@

CLEANFILES = ${PYTHON_TOOLS}
bin_SCRIPTS = ${PYTHON_TOOLS}

EXTRA_DIST = $(nobase_pythonlib_DATA) $(pythonlib_SCRIPTS) docs LICENSE

clean-local:
	rm -f membase*tar.gz
	rm -rf $(TMP_DIR)

bdist:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)
	rm -f ./couchbase-cli_*.tar.gz
	rm -rf $(TMP_DIR)/couchbase-cli
	mkdir -p $(TMP_DIR)/couchbase-cli/simplejson
	cp $(pythonlib_SCRIPTS) *.py LICENSE $(TMP_DIR)/couchbase-cli
	cp simplejson/*.py $(TMP_DIR)/couchbase-cli/simplejson
	cp simplejson/LICENSE.txt $(TMP_DIR)/couchbase-cli/simplejson
	(cd $(TMP_DIR); tar cf - couchbase-cli) | gzip -9 > couchbase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	echo created couchbase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	rm -rf $(TMP_DIR)

