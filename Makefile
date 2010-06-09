TMP_DIR = ./tmp
TMP_VER = $(TMP_DIR)/version_num.tmp

clean:
	rm *.pyc
	rm membase*tar.gz
	rm -rf $(TMP_DIR)

bdist:
	test -d $(TMP_DIR) || mkdir $(TMP_DIR)
	git describe | sed s/-/_/g > $(TMP_VER)
	rm -f ./membase-cli_*.tar.gz
	rm -rf $(TMP_DIR)/membase-cli
	mkdir $(TMP_DIR)/membase-cli
	cp membase *.py README COPYING $(TMP_DIR)/membase-cli
	(cd $(TMP_DIR); tar cf - membase-cli) | gzip -9 > membase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	echo created membase-cli_`cat $(TMP_VER)`-`uname -s`.`uname -m`.tar.gz
	rm -rf $(TMP_DIR)

