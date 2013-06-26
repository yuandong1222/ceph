#!/usr/bin/python

from REST_metadata_syncher import REST_metadata_syncher

# this instantiates a MetadataSyncher and kicks off a synch of all shards in a repo
# hard-coded only for testing
#access_key_source = '95OKY95S2US1LB4Z4ZYI' #buck
#secret_key_source = 'zy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByl' #buck
access_key_source = 'X2IYPSTY1072DDY1SJMC' # rgw-n1-z1 ssytem user
secret_key_source = 'YIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm'   #rgw-n1-z1 system user
host_source = 'rgw-n1'
source_zone = 'rgw-n1-z1'

#access_key_dest = '95OKY95S2US1LB4Z4ZYI' #buck
#secret_key_dest = 'zy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByl' #buck
access_key_dest = 'X2IYPSTY1072DDY1ABCD' #rgw-n2-z1 system user
secret_key_dest = 'ABCDICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm' #rgw-n2-z1
host_dest = 'rgw-n2'
dest_zone = 'rgw-n2-z1'
#log_lock_time = 120 # 2 minute log lock cycles
log_lock_time = 15 # 15 seconds, just for  testing


if __name__ == '__main__':
    #syncher = REST_metadata_syncher(access_key_source, secret_key_source, host_source, source_zone,
    #                                access_key_dest, secret_key_dest, host_dest, dest_zone)
    syncher = REST_metadata_syncher()

    # sync all the data
    syncher.sync_all_shards(access_key_source, secret_key_source, host_source, source_zone,
                            access_key_dest, secret_key_dest, host_dest, dest_zone, 4, log_lock_time)

    #syncher.sync_all_shards()
