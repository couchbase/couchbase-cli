#!/usr/bin/env python

import memcacheConstants
import pump

class GenSource(pump.Source):
    """Generates simple SET/GET workload, useful for basic testing.
       Examples:
         ./cbtransfer gen: http://10.3.121.192:8091
         ./cbtransfer gen:max-items=50000,min-value-size=10,exit-after-creates=1,\
                          prefix=steve1-,ratio-sets=1.0 \
                      http://10.3.121.192:8091 -B my-other-bucket --threads=10
    """
    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(GenSource, self).__init__(opts, spec, source_bucket, source_node,
                                        source_map, sink_map, ctl, cur)
        self.done = False
        self.body = None
        self.cur_ops = source_map['cfg']['cur-ops']
        self.cur_gets = source_map['cfg']['cur-gets']
        self.cur_sets = source_map['cfg']['cur-sets']
        self.cur_items = source_map['cfg']['cur-items']

    @staticmethod
    def can_handle(opts, spec):
        """The gen spec follows gen:[key=value[,key=value]] format."""
        return spec.startswith("gen:")

    @staticmethod
    def check(opts, spec):
        rv, cfg = GenSource.parse_spec(opts, spec)
        if rv != 0:
            return rv, None
        return 0, {'cfg': cfg,
                   'spec': spec,
                   'buckets': [{'name': 'default',
                                'nodes': [{'hostname': 'N/A-' + str(i)}
                                          for i in range(opts.threads)]}]}

    @staticmethod
    def parse_spec(opts, spec):
        """Parse the comma-separated key=value configuration from the gen spec.
           Names and semantics were inspired from subset of mcsoda parameters."""
        cfg = {'cur-ops': 0,
               'cur-gets': 0,
               'cur-sets': 0,
               'cur-items': 0,
               'exit-after-creates': 0,
               'max-items': 10000,
               'min-value-size': 10,
               'prefix': "",
               'ratio-sets': 0.05,
               'json': 0}
        for kv in spec[len("gen:"):].split(','):
            if kv:
                k = kv.split('=')[0].strip()
                v = kv.split('=')[1].strip()
                try:
                    if k in cfg:
                        cfg[k] = type(cfg[k])(v)
                    else:
                        return "error: unknown workload gen parameter: %s" % (k), None
                except ValueError:
                    return "error: could not parse value from: %s" % (kv), None
        return 0, cfg

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        """No design from a GenSource."""
        return 0, None

    def provide_batch(self):
        """Provides a batch of messages, with GET/SET ratios and keys
           controlled by a mcsoda-inspired approach, but simpler."""
        if self.done:
            return 0, None

        cfg = self.source_map['cfg']
        prefix = cfg['prefix']
        max_items = cfg['max-items']
        ratio_sets = cfg['ratio-sets']
        exit_after_creates = cfg['exit-after-creates']
        json = cfg['json']
        if not self.body:
            min_value_body = "0" * cfg['min-value-size']
            if json:
                self.body = '{"name": "%s%s", "age": %s, "index": %s,' + \
                            ' "body": "%s"}' % min_value_body
            else:
                self.body = min_value_body

        batch = pump.Batch(self)

        batch_max_size = self.opts.extra['batch_max_size']
        batch_max_bytes = self.opts.extra['batch_max_bytes']

        vbucket_id = 0x0000ffff
        cas, exp, flg = 0, 0, 0

        while (batch.size() < batch_max_size and
               batch.bytes < batch_max_bytes):
            if ratio_sets >= float(self.cur_sets) / float(self.cur_ops or 1):
                self.cur_sets = self.cur_sets + 1
                cmd = memcacheConstants.CMD_TAP_MUTATION
                if self.cur_items < max_items:
                    key = self.cur_items
                    self.cur_items = self.cur_items + 1
                else:
                    key = self.cur_sets % self.cur_items
            else:
                self.cur_gets = self.cur_gets + 1
                cmd = memcacheConstants.CMD_GET
                key = self.cur_gets % self.cur_items
            self.cur_ops = self.cur_ops + 1

            if json:
                value = self.body % (prefix, key, key % 101, key)
            else:
                value = self.body
            msg = (cmd, vbucket_id, prefix + str(key), flg, exp, cas, '', value)
            batch.append(msg, len(value))

            if exit_after_creates and self.cur_items >= max_items:
                self.done = True
                return 0, batch

        if batch.size() <= 0:
            return 0, None
        return 0, batch

    @staticmethod
    def total_msgs(opts, source_bucket, source_node, source_map):
        """Returns max-items only if exit-after-creates was specified.
           Else, total msgs is unknown as GenSource does not stop generating."""
        if source_map['cfg']['exit-after-creates'] and source_map['cfg']['ratio-sets'] > 0:
            total_ops = source_map['cfg']['max-items'] / source_map['cfg']['ratio-sets']
            return 0, int(total_ops)
        return 0, None
