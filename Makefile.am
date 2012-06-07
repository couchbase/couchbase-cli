TMP_DIR = ./tmp
TMP_VER = $(TMP_DIR)/version_num.tmp

default:


pythonlibdir=$(libdir)/python
nobase_pythonlib_DATA= \
         buckets.py \
         cluster_stats.py \
         collector.py \
         diskqueue_stats.py \
         info.py \
         listservers.py \
         node.py \
         node_stats.py \
         processor.py \
         restclient.py \
         simplejson/LICENSE.txt \
         simplejson/__init__.py \
         simplejson/decoder.py \
         simplejson/encoder.py \
         simplejson/scanner.py \
         stats_buffer.py \
         usage.py \
         util_cli.py

pythonlib_SCRIPTS= couchbase-cli cbclusterstats

PYTHON_TOOLS= wrapper/couchbase-cli

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
	rm -f ./membase-cli_*.tar.gz
	rm -rf $(TMP_DIR)/membase-cli
	mkdir -p $(TMP_DIR)/membase-cli/simplejson
	cp couchbase-cli *.py COPYING $(TMP_DIR)/membase-cli
	cp simplejson/*.py $(TMP_DIR)/membase-cli/simplejson
	cp simplejson/LICENSE.txt $(TMP_DIR)/membase-cli/simplejson
	(cd $(TMP_DIR); tar cf - membase-cli) | gzip -9 > membase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	echo created membase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	rm -rf $(TMP_DIR)

