nodes = {}
node_stats = {}

buckets_summary = {}
stats_summary = {}

bucket_info = {}
buckets = {}
stats = {
    "minute" : {
        'disk_write_queue' : {},
        'cmd_get' : {},
        'cmd_set' : {},
        'delete_hits' : {},
        'curr_items' : {},
        'vb_replica_curr_items' : {},
        'curr_connections' : {},
        'vb_active_queue_drain' : {},
        'vb_replica_queue_drain' : {},
        'disk_write_queue' : {},
    },
    "hour" : {
        'disk_write_queue' : {},
        'ep_cache_miss_rate' : {},
        'ep_tap_total_total_backlog_size' : { },
        'ep_oom_errors' : {},
        'ep_tmp_oom_errors' : {},
        'vb_active_num' : {},
        'vb_replica_num' : {},
        "mem_used" : {},
    },
    "day" : {
        'curr_items' : {},
    },
}