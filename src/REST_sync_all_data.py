#!/usr/bin/python

from REST_data_syncer import REST_data_syncer

# this instantiates a MetadataSyncher and kicks off a sync of all shards in a repo
# hard-coded only for testing
#access_key_source = '95OKY95S2US1LB4Z4ZYI' #buck
#secret_key_source = 'zy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByl'
access_key_source = 'X2IYPSTY1072DDY1SJMC' # rgw-n1-z1
secret_key_source = 'YIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm' # rgw-n1-z1
host_source = 'rgw-n1'
zone_source = 'rgw-n1-z1'

#access_key_dest = '85OKY95S2US1LB4Z4ZYI'
#secret_key_dest = 'wy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByk'
access_key_dest = '2X2IYPSTY1072DDY1SJMC' # rgw-n2-z1
secret_key_dest = '2YIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm' # rgw-n2-z1
host_dest = 'rgw-n2'
zone_dest = 'rgw-n2-z1'

log_lock_time = 15 # seconds, ( 15 for testing, normally 2 - 5 minutes)
num_workers = 1

if __name__ == '__main__':
    syncer = REST_data_syncer()

    # sync all the data
    syncer.sync_all_buckets( access_key_source, secret_key_source, host_source, \
                             zone_source, access_key_dest, secret_key_dest, \
                             host_dest, zone_dest, num_workers, log_lock_time)

    print 'in data_syncer().sync_all_buckets(), all done'
