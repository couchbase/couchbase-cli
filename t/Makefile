TEST_SOURCE=10.3.121.192
TEST_SINK=10.3.121.192

test-unit:
	./t/pump_dcp_test.py -v

test-dev: test-clean test-backup test-transfer \
          test-restore-cb test-restore-mc

test-clean:
	rm -rf /tmp/b /tmp/c

test-backup:
	./cbbackup http://$(TEST_SOURCE):8091 /tmp/b -v -v

test-transfer:
	./cbtransfer /tmp/b /tmp/c -v -v

test-restore-cb:
	./cbrestore /tmp/c couchbase://Administrator:password@$(TEST_SINK):8091 \
                -b default -v -v

test-restore-mc:
	./cbrestore /tmp/c memcached://$(TEST_SINK):11211 -v -v

todo:
	git grep TODO: | cut -f 2 -d '#' | cut -f 2 -d ':'

wc:
	wc -l pump*.py cbbackup cbrestore cbtransfer
	wc -l t/pump_test.py

help:
	./cbbackup -h
	echo ------------------------------------------------
	./cbrestore -h
	echo ------------------------------------------------
	./cbtransfer -h
