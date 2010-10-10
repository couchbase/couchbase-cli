TMP_DIR = ./tmp
TMP_VER = $(TMP_DIR)/version_num.tmp

default:


PREFIX=/opt/membase/bin/cli
${PREFIX}/%.py: %.py
	cp $< $@

${PREFIX}:; -@mkdir $@

${PREFIX}/simplejson: simplejson
	cp -r simplejson $@

${PREFIX}/membase: ${PREFIX} membase
	cp membase $@

install: ${PREFIX} \
         ${PREFIX}/buckets.py \
         ${PREFIX}/info.py \
         ${PREFIX}/listservers.py \
         ${PREFIX}/membase \
         ${PREFIX}/node.py \
         ${PREFIX}/restclient.py \
         ${PREFIX}/usage.py \
         ${PREFIX}/util.py \
         ${PREFIX}/simplejson

clean:
	rm *.pyc
	rm membase*tar.gz
	rm -rf $(TMP_DIR)

bdist:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)
	rm -f ./membase-cli_*.tar.gz
	rm -rf $(TMP_DIR)/membase-cli
	mkdir -p $(TMP_DIR)/membase-cli/simplejson
	cp membase *.py COPYING $(TMP_DIR)/membase-cli
	cp simplejson/*.py $(TMP_DIR)/membase-cli/simplejson
	cp simplejson/LICENSE.txt $(TMP_DIR)/membase-cli/simplejson
	(cd $(TMP_DIR); tar cf - membase-cli) | gzip -9 > membase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	echo created membase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	rm -rf $(TMP_DIR)

