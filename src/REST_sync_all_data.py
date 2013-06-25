#!/usr/bin/python

from REST_data_syncher import REST_data_syncher

# this instantiates a MetadataSyncher and kicks off a synch of all shards in a repo
# hard-coded only for testing
#access_key_source = '95OKY95S2US1LB4Z4ZYI' #buck
#secret_key_source = 'zy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByl'
access_key_source = 'X2IYPSTY1072DDY1SJMC' # rgw-n1-z1
secret_key_source = 'YIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm' # rgw-n1-z1
host_source = 'rgw-n1'
#access_key_dest = '85OKY95S2US1LB4Z4ZYI'
#secret_key_dest = 'wy4DkpcGG+fAuOMfYPYAI37P7Qe2kg+oQpZIqByk'
access_key_dest = '2X2IYPSTY1072DDY1SJMC' # rgw-n2-z1
secret_key_dest = '2YIMHICpPvT+MhLTbSsiBJ1jQF15IFvJA8tgwJEcm' # rgw-n2-z1
host_dest = 'rgw-n2'

if __name__ == '__main__':
    syncher = REST_data_syncher(access_key_source, secret_key_source, host_source,
                                access_key_dest, secret_key_dest, host_dest)

    # sync all the data
    syncher.sync_all_buckets()
