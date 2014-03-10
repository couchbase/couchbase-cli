import pump
import pump_bfd

class BFDSinkEx(pump_bfd.BFDSink):

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(pump_bfd.BFDSink, self).__init__(opts, spec, source_bucket, source_node,
                                      source_map, sink_map, ctl, cur)
        self.mode = getattr(opts, "mode", "diff")
        self.init_worker(pump_bfd.BFDSink.run)

    @staticmethod
    def check_spec(source_bucket, source_node, opts, spec, ctl):
        pump.Sink.check_spec(source_bucket, source_node, opts, spec, ctl)

        seqno, dep = pump_bfd.BFD.find_seqno(opts, spec,
                                    source_bucket['name'],
                                    source_node['hostname'],
                                    getattr(opts, "mode", "diff"))
        if seqno:
            ctl['seqno'] = seqno
