#!/usr/bin/env python

import logging
import os
import simplejson as json
import struct
import sys
import shutil
import tempfile
import zipfile

import couchbaseConstants
import pump

JSON_SCHEME = "json://"

class ZipUtil:
    def __init__(self, zipobj):
        self.zipobj = zipobj

    def extractall(self, path=None):
        if path is None:
            path = os.getcwd()
        if (path[-1] in (os.path.sep, os.path.altsep)
            and len(os.path.splitdrive(path)[1]) > 1):
            path = path[:-1]

        for member in self.zipobj.namelist():
            if not isinstance(member, zipfile.ZipInfo):
                member = self.zipobj.getinfo(member)

            # don't include leading "/" from file name if present
            if member.filename[0] == '/':
                targetpath = os.path.join(path, member.filename[1:])
            else:
                targetpath = os.path.join(path, member.filename)

            targetpath = os.path.normpath(targetpath)

            # Create all parent directories if necessary.
            upperdirs = os.path.dirname(targetpath)
            if upperdirs and not os.path.exists(upperdirs):
                try:
                    os.makedirs(upperdirs)
                except:
                    logging.error("Unexpected error:" + sys.exc_info()[0])
                    return upperdirs

            if member.filename[-1] == '/':
                if not os.path.isdir(targetpath):
                    try:
                        os.mkdir(targetpath)
                    except:
                        logging.error("Fail to create directory:" + targetpath)
                continue

            target = file(targetpath, "wb")
            target.write(self.zipobj.read(member.filename))
            target.close()

        return path

class JSONSource(pump.Source):
    """Reads json file or directory or zip file that contains json files."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(JSONSource, self).__init__(opts, spec, source_bucket, source_node,
                                         source_map, sink_map, ctl, cur)
        self.done = False
        self.f = None
        self.views = list()

    @staticmethod
    def can_handle(opts, spec):
        return spec.startswith(JSON_SCHEME) and \
            (os.path.isfile(spec.replace(JSON_SCHEME, "")) or \
             os.path.isdir(spec.replace(JSON_SCHEME, "")) or \
             spec.endswith(".zip"))

    @staticmethod
    def check(opts, spec):
        return 0, {'spec': spec,
                   'buckets': [{'name': os.path.basename(spec),
                                'nodes': [{'hostname': 'N/A'}]}]}
    @staticmethod
    def save_doc(batch, dockey, datafile, is_data):
        cmd = couchbaseConstants.CMD_TAP_MUTATION
        vbucket_id = 0x0000ffff
        cas, exp, flg = 0, 0, 0
        try:
            raw_data = datafile.read()
            doc = json.loads(raw_data)
            if '_id' not in doc:
                if is_data:
                    msg = (cmd, vbucket_id, dockey, flg, exp, cas, '', raw_data, 0, 0, 0)
                    batch.append(msg, len(raw_data))
            else:
                id = doc['_id'].encode('UTF-8')
                del doc['_id']
                docdata = {"doc":{
                    "json": doc,
                    "meta":{"id":id}
                }}
                if not is_data:
                    batch.append(json.dumps(docdata), len(docdata))
        except ValueError, error:
            logging.error("Fail to read json file with error:" + str(error))

    @staticmethod
    def gen_dockey(filename):
        return os.path.splitext(os.path.basename(filename))[0]

    @staticmethod
    def enumerate_and_save(batch, subdir, is_data):
        if not subdir:
            return
        subdirlist = list()
        viewdirs = list()
        for item in os.listdir(subdir):
            if os.path.isfile(os.path.join(subdir, item)):
                try:
                    fp = open(os.path.join(subdir, item), 'r')
                    dockey = JSONSource.gen_dockey(item)
                    JSONSource.save_doc(batch, dockey, fp, is_data)
                    fp.close()
                except IOError, error:
                    logging.error("Fail to load json file with error" + str(error))
            else:
                if item.find("design_docs") > 0:
                    viewdirs.append(os.path.join(subdir, item))
                else:
                    subdirlist.append(os.path.join(subdir, item))
        for dir in subdirlist:
            JSONSource.enumerate_and_save(batch, dir, is_data)
        for dir in viewdirs:
            JSONSource.enumerate_and_save(batch, dir, is_data)

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        return 0, None

    def provide_batch(self):
        if self.done:
            return 0, None

        if not self.f:
            self.f = self.spec.replace(JSON_SCHEME, "")
        else:
            return 0, None

        batch = pump.Batch(self)
        if self.f:
            if os.path.isfile(self.f) and self.f.endswith(".zip"):
                zfobj = zipfile.ZipFile(self.f)
                working_dir = tempfile.mkdtemp()
                ZipUtil(zfobj).extractall(working_dir)
                JSONSource.enumerate_and_save(batch, working_dir, True)
                shutil.rmtree(working_dir)
            elif os.path.isdir(self.f):
                JSONSource.enumerate_and_save(batch, self.f, True)
            else:
                try:
                    fp = open(self.f, 'r')
                    dockey = JSONSource.gen_dockey(os.path.basename(self.f))
                    JSONSource.save_doc(batch, dockey, fp)
                    fp.close()
                except IOError, error:
                    return "error: could not open json: %s; exception: %s" % \
                        (self.f, e), None

        if batch.size() <= 0:
            return 0, None
        return 0, batch
