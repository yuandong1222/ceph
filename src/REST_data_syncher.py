
import boto
import datetime
import json
import logging
import random
#import requests
import string
import sys
import threading
import time

import boto.s3.acl
import boto.s3.connection

from boto.connection import AWSAuthConnection
from operator import attrgetter
from RGW_REST_factory import RGW_REST_factory

log_lock_time = 60 #seconds

#debug_commands = True
debug_commands = False
#deep_compare = True
deep_compare = False

class REST_data_syncher:
    logging.basicConfig(filename='boto_data_syncher.log', level=logging.DEBUG)
    log = logging.getLogger(__name__)


    source_conn = None
    dest_conn = None
    local_lock_id = None
    rest_factory = None

    def __init__(self, source_access_key, source_secret_key, source_host,
                       dest_access_key, dest_secret_key, dest_host):

        # generates an N character random string from letters and digits 16 digits for now
        self.local_lock_id = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(16))
    
        # construct the two connection objects
        self.source_conn = boto.s3.connection.S3Connection(
            aws_access_key_id = source_access_key,
            aws_secret_access_key = source_secret_key,
            is_secure=False,
            host = source_host,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            debug=2
        )

        self.dest_conn = boto.s3.connection.S3Connection(
          aws_access_key_id = dest_access_key,
          aws_secret_access_key = dest_secret_key,
          is_secure=False,
          host = dest_host,
          calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

        self.rest_factory = RGW_REST_factory()


    # we explicitly specify the connection to use for the locking here  
    # in case we need to lock a non-master log file
    def acquire_log_lock(self, conn, lock_id, bucket_num):
        (ret, out) = self.rest_factory.rest_call(conn, ['log', 'lock'], 
            {"type":"data", "id":bucket_num, "length":log_lock_time, "lock_id":lock_id})

        if 200 != ret:
            print 'acquire_log_lock failed, returned http code: ', ret
        elif debug_commands:
            print 'acquire_log_lock returned: ', ret

        return ret

    # we explicitly specify the connection to use for the locking here  
    # in case we need to lock a non-master log file
    def release_log_lock(self, conn, lock_id, bucket_num):
        (ret, out) = self.rest_factory.rest_call(self.source_conn, ['log', 'unlock'], {
                                   "type":"data", "id":bucket_num, "lock_id":self.local_lock_id})

        if 200 != ret:
            print 'data log unlock failed, returned http code: ', ret
        elif debug_commands:
            print 'data log unlock returned: ', ret

        return ret


    # copies the curret data for a user from the master side to the 
    # non-master side
    def add_user_to_remote(self, uid):

        # get current info for this uid. 
        (ret, src_acct) = self.rest_factory.rest_call(self.source_conn, 
                                ['data', 'userget'], {'key':uid})
        if 200 != ret:
            print 'add_user_to_remote source side data user (GET) failed, code: ', ret
            return ret
        elif debug_commands:
            print 'add_user_to_remote data userget() returned: ', ret

        # create an empty dict and pull out the user_id to use as an argument for next call
        args = {}
        args['key'] = src_acct()['data']['user_id']

        # json encode the data
        outData = json.dumps(src_acct())

        (ret, dest_acct) = self.rest_factory.rest_call(self.dest_conn, 
                                ['data', 'userput'], args, data=outData)
        if 200 != ret:
            print 'data user (PUT) failed, return http code: ', ret
            print 'body: ', dest_acct()
            return ret
        elif debug_commands:
            print 'add_user_to_remote data userput() returned: ', ret


        return ret

     # for now, just reuse the add_user code. May need to differentiate in the future
    def update_remote_user(self, uid):
        return self.add_user_to_remote(uid)


    # use the specified connection as it may be pulling data from any rgw
    def pull_data_for_uid(self, conn, uid):
        (retVal, out) = self.rest_factory.rest_call(conn, ['data', 'userget'], {"key":uid})
        if 200 != retVal and 404 != retVal:
            print 'data user(GET) failed for {uid} returned {val}'.format(uid=uid,val=retVal)
            return retVal, None
        else:
            if debug_commands:
                print 'pull_data_for_uid for {uid} returned: {val}'.format(uid=uid,val=retVal)

            return retVal, out()

    # only used for debugging at present
    def manually_diff(self, source_data, dest_data):
        diffkeys1 = [k for k in source_data if source_data[k] != dest_data[k]]
        diffkeys2 = [k for k in dest_data if dest_data[k] != source_data[k] and not (k in source_data) ]
    
        return diffkeys1 + diffkeys2

    def manually_diff_individual_user(self, uid):
        retVal = 200 # default to success
   
        # get user data from the source side
        (retVal, source_data) = self.pull_data_for_uid(self.source_conn, uid)
        if 200 != retVal:
            return retVal

        # get the data for the same uid from the dest side
        (retVal, dest_data) = self.pull_data_for_uid(self.dest_conn, uid)
        if 200 != retVal:
            return retVal

        diff_set = self.manually_diff(source_data, dest_data)
   
        if 0 == len(diff_set):
            print 'deep comparison of uid ', uid, ' passed'
        else:
            for k in diff_set:
                print k, ':', source_data[k], ' != ', dest_data[k]
                retVal = 400 # throw an http-ish error code
   
        return retVal

    # for a given user, check to see 
    def check_individual_user(self, uid, tag=None):
        retVal = 200

        retVal, source_data = self.pull_data_for_uid(self.source_conn, uid)
        if 200 != retVal:
            return retVal

        # If the tag in the data log on the source_data does not match the current tag
        # for the uid, skip this user.
        # We assume this is caused by this entry being for a uid that has been deleted.
        if tag != None and tag != source_data['ver']['tag']:
            print 'log tag ', tag, ' != current uid tag ', source_data['ver']['tag'], \
                  '. Skipping this uid / tag pair (', uid, " / ", tag, ")"
            return retVal

        # if the user does not exist on the non-master side, then a 404 
        # return value is appropriate
        retVal, dest_data = self.pull_data_for_uid(self.dest_conn, uid)
        if 200 != retVal and 404 != retVal:
            return retVal

        # if this user does not exist on the destination side, add it
        if (retVal == 404 and dest_data['Code'] == 'NoSuchKey'):
            print 'user: ', uid, ' missing from the remote side. Adding it'
            retVal = self.add_user_to_remote(uid)

        else: # if the user exists on the remote side, ensure they're the same version
            dest_ver = dest_data['ver']['ver']
            source_ver = source_data['ver']['ver']

            if dest_ver != source_ver:
                print 'uid: ', uid, ' local_ver: ', source_ver, ' != dest_ver: ', dest_ver, ' UPDATING'
                retVal = self.update_remote_user(self.source_conn, self.dest_conn, uid)
            elif debug_commands:
                print 'uid: ', uid, ' local_ver: ', source_ver, ' == dest_ver: ', dest_ver 

        return retVal


    def set_datalog_work_bound(self, bucket_num, time_to_use):
        (ret, out) = self.rest_factory.rest_call(self.source_conn, 
                               ['replica_log', 'set', 'work_bound'], 
                      {"id":bucket_num, "type":"data", "marker":"FIIK", }) 

        if 200 != ret:
            print 'data list failed, returned http code: ', ret
        elif debug_commands:
            print 'data list returned: ', ret

    # get the updates for this bucket and sync the data across
    def sync_bucket(self, shard, bucket_name):

        #dummy_marker = 'buck'
        #dummy_marker = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(16))

        # first, get the bilog
        (ret, out) = self.rest_factory.rest_call(self.source_conn, 
                               ['log', 'list', 'type=bucket-index'], 
                      #{"bucket":bucket_name, 'marker':dummy_marker }) 
                      {"bucket":bucket_name}) 

        if 200 != ret:
            print 'get bucket-index failed, returned http code: ', ret
        elif debug_commands:
            print 'get bucket-index returned: ', ret

        bucket_events = out()

        print 'bilog for bucket ', bucket_name, ' has ', len(bucket_events), ' entries'
        # first, make sure the events are sorted in index_ver order 
        sorted_events = sorted(bucket_events, key=lambda entry: entry['index_ver']) 
                          #reverse=True)

        for event in sorted_events:
            if (event['state'] == 'complete'):
              print '   applying: ', event


    # data changes are grouped into buckets based on [ uid | tag ] TODO, figure this out
    def process_bucket(self, shard):
        print 'processing updated buckets list shard ', shard
        # we need this due to a bug in rgw that isn't auto-filling in sensible defaults
        # when start-time is omitted
        really_old_time = "2010-10-10 12:12:00"

        # NOTE rgw deals in UTC time. Make sure you adjust your calls accordingly
        sync_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        print 'acquiring data log lock'
        # first, lock the log
        self.acquire_log_lock(self.source_conn, self.local_lock_id, shard)


        #(ret, out) = self.rest_factory.rest_call(self.source_conn, 
        #                       ['log', 'list', 'id=' + str(bucket_num)], 
        #              {"id":bucket_num, "type":"data", 
        #               "start-time":really_old_time, 
        #               "end-time":sync_start_time})
        (ret, out) = self.rest_factory.rest_call(self.source_conn, 
                               ['log', 'list', 'id=' + str(shard)], 
                      {"id":shard, "type":"data"}) 
        #              {"id":bucket_num, "type":"data", "start-time":really_old_time, 
        #               "end-time":sync_start_time})

        if 200 != ret:
            print 'data list failed, returned http code: ', ret
        elif debug_commands:
            print 'data list returned: ', ret

        data_updates_list = out()
        buckets_to_sync = {}

        print 'updated log shard ', shard, ' has ', len(data_updates_list), ' entries'
        for entry in data_updates_list:
            #print '   log entry:', entry
            if buckets_to_sync.has_key(entry['key']):
                pass
            else:
                buckets_to_sync[entry['key']] = ""

        for entry in buckets_to_sync:
            print '   buckets to sync: ', entry
            retVal = self.sync_bucket(shard, entry)
            if 200 != ret:
                print 'sync_bucket returned http code ', ret
            elif debug_commands:
                print 'sync_bucket returned http code ', ret

        #print 'set the datalog work bound'
        #self.set_datalog_work_bound(bucket_num, sync_start_time)

        # trim the log for this bucket now that all the users are synched
        #(ret, out) = self.rest_factory.rest_call(self.source_conn, ['log', 'trim', 'id=' + str(bucket_num)], 
        #              {"id":bucket_num, "type":"data", "start-time":really_old_time, 
        #               "end-time":sync_start_time})

        #if 200 != ret:
        #    print 'log trim returned http code ', ret
        #elif debug_commands:
        #    print 'log trim returned http code ', ret

        print 'unlocking data log'
        # finally, unlock the log
        self.release_log_lock(self.source_conn, self.local_lock_id, shard)

    # queries the number of buckets and then synches each bucket, one at a time
    def sync_all_buckets(self):
        (ret, out) = self.rest_factory.rest_call(self.source_conn, 
                            ['log', 'list', 'type=data'], {"type":"data"})

        if 200 != ret:
            print 'log list type:data failed, code: ', ret
        elif debug_commands:
            print 'log list type:data returned code: ', ret
            print 'out: ', out()


        numObjects = out()['num_objects']
        print 'We have ', numObjects, ' buckets to check'

        for i in xrange(numObjects):
            self.process_bucket(i)

    # synchs a single uid
    def sync_one_user(self, uid):
        retVal = self.check_individual_user(uid)

    # acquires a list of all the uids on the source side and then, for each one
    # compares the individual data entries between the source and destination RGWs.
    # discrepencies are printed out
    def manually_diff_all_users(self):
        # get the source accounts
        (ret, src_accts) = self.rest_factory.rest_call(self.source_conn, 
                                ['data', 'userget'], {})
        if 200 != ret:
            print 'manually_diff_all_users() source side data user (GET) failed, code: ', ret
            return ret
        elif debug_commands:
            print 'add_user_to_remote data userget() returned: ', ret

        for uid in src_accts():
            self.manually_diff_individual_user(uid)
            
