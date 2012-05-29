SOURCE=10.3.121.192
SINK=10.3.121.192

test: test-clean test-unit test-backup test-transfer test-restore-cb test-restore-mc

test-clean:
	rm -rf /tmp/b /tmp/c

test-unit:
	./t/pump_test.py -v

test-backup:
	./cbbackup http://$(SOURCE):8091 /tmp/b -v -v

test-transfer:
	./cbtransfer /tmp/b /tmp/c -v -v

test-restore-cb:
	./cbrestore /tmp/c couchbase://Administrator:password@$(SINK):8091 \
            -b default -v -v

test-restore-mc:
	./cbrestore /tmp/c memcached://$(SINK):11211 -v -v

todo:
	git grep TODO: | cut -f 2 -d '#' | cut -f 2 -d ':'

wc:
	wc -l pump*.py cbbackup cbrestore cbtransfer

help:
	./cbbackup -h
	echo ------------------------------------------------
	./cbrestore -h
