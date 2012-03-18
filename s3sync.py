#!/usr/bin/env python2.6
import os
import boto
from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.connection import Key
from optparse import OptionParser

def parse_options():
    parser = OptionParser()
    parser.add_option("-s", "--source", 
                      action="store", type="string", 
                      help="source file or directory to sync")
    parser.add_option("-b", "--bucket",
                      action="store", type="string",
                      help="destination bucket in s3")
    parser.add_option("-k", "--awskey",
                      action="store", type="string",
                      help="aws key")
    parser.add_option("-a", "--awssecret",
                      action="store", type="string",
                      help="aws secret")
   
   
   
    (options, args) = parser.parse_args()
    
    return options.source, options.bucket, options.awskey, options.awssecret

class S3Bucket():
    def __init__(self, aws_key, aws_secret, site_location):
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.site_location = site_location

    def create_connection(self):
        self.conn = S3Connection(self.aws_key, 
                                 self.aws_secret)

    def create_bucket(self, name):
        try:
            bucket_id = self.conn.create_bucket(name, 
                                                location=self.site_location)
        except boto.exception.S3CreateError:
            raise
        except boto.exception.S3ResponseError:
            raise

        return bucket_id

    def get_bucket(self, bucket):
        bucket_id = self.conn.get_bucket(bucket)
        
        return bucket_id

    def sync_dir(self, directory, bucket_id):
        k = Key(bucket_id)
        tree = self._get_tree(directory)
        for fname in tree:
           k.key = fname
           k.set_contents_from_filename(fname)

    def _get_tree(self, directory):
        tree = []
        for root, dirs, files in os.walk(directory):
            for name in files:
                tree.append(os.path.join(root, name))
               
        return tree


if __name__ == '__main__':
    source_dir, bucket, aws_key, aws_secret = parse_options()
 
    site_location = Location.USWest

    s3_bucket = S3Bucket(aws_key, aws_secret, site_location)

    s3_bucket.create_connection()
    try:
        bucket_id = s3_bucket.create_bucket(bucket)
    except boto.exception.S3CreateError:
        bucket_id = s3_bucket.get_bucket(bucket)
    except boto.exception.S3ResponseError:
        print "invalid region specified"
    
    s3_bucket.sync_dir(source_dir, bucket_id)
    
