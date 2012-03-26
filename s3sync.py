#!/usr/bin/env python
import os
import sys
import boto
import logging
from boto.s3.connection import S3Connection
from boto.s3.connection import Location
from boto.s3.connection import Key
from optparse import OptionParser

LOG = logging.getLogger(__name__)

def parse_options():
    parser = OptionParser()
    parser.add_option("-s", "--source", default=".",
                      action="store", type="string", 
                      help="source file or directory to sync")
    parser.add_option("-b", "--bucket",
                      action="store", type="string",
                      help="destination bucket in s3")
    parser.add_option("-k", "--awskey",
                      action="store", type="string",
                      help="aws key")
    parser.add_option("-l", "--loglevel", default="INFO",
                      action="store", type="string",
                      help="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    parser.add_option("-a", "--awssecret",
                      action="store", type="string",
                      help="aws secret")
    parser.add_option("-r", "--region", default=Location.USWest,
                      action="store",
                      help="Select S3 return")
   
    options, args = parser.parse_args()
    
    return (options, args)

class S3Bucket(object):

    def __init__(self, aws_key, aws_secret, bucket_name, 
                 region=Location.USWest, source_dir=".", follow_links=False):
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region
        self.bucket_name = bucket_name
        self.source_dir = source_dir
        self.follow_links = follow_links
        self.tree = None
        #self.conn = S3Connection(self.aws_key, self.aws_secret)
        #self.bucket_id = self.conn.create_bucket(bucket_name, 
        #                    location=self.site_location)

    def sync_data(self, disable_checksum=False):
        for fname in self.tree:
            LOG.debug(fname)
            #k = Key(self.bucket_id)
            #k.key = fname
            #k.set_contents_from_filename(fname)

    def process(self):
        if self.tree is None:
            self.get_tree()
        self.sync_data()

    def get_tree(self):
        LOG.debug("Walking tree rooted at: %s" % self.source_dir)
        tree = []
        for root, dirs, files in os.walk(self.source_dir, followlinks=self.follow_links):
            for name in files:
                tree.append(os.path.join(root, name))

        self.tree = tree


if __name__ == '__main__':
    options, args = parse_options()

    # Convenience logger for command line testing.
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, options.loglevel.upper()))

    s3_bucket = S3Bucket(options.awskey, options.awssecret, 
                    options.bucket, region=options.region, source_dir=options.source)
    s3_bucket.process()
    
