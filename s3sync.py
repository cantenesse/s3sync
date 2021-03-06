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
    def __init__(self, aws_key, aws_secret, site_location, bucket):
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.site_location = site_location
        self.conn = S3Connection(self.aws_key, 
                                 self.aws_secret)
        try:
            self.bucket_id = self.conn.create_bucket(bucket, 
                                                location=self.site_location)
        except boto.exception.S3CreateError:
            self.bucket_id = self.conn.get_bucket(bucket)

    def sync_dir(self, directory):
        k = Key(self.bucket_id)
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

    s3_bucket = S3Bucket(aws_key, aws_secret, site_location, bucket)

    s3_bucket.sync_dir(source_dir)
    
