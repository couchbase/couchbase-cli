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
        self.file_iter = None
        self.working_dir = None

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

    def save_doc(self, batch, dockey, datafile, is_data):
        cmd = couchbaseConstants.CMD_TAP_MUTATION
        vbucket_id = 0x0000ffff
        cas, exp, flg = 0, 0, 0
        try:
            raw_data = datafile.read()
            doc = json.loads(raw_data)
            if '_id' not in doc:
                if is_data:
                    msg = (cmd, vbucket_id, dockey, flg, exp, cas, '', raw_data, 0, 0, 0, 0)
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
    def enumerate_files(subdir, file_candidate, skip_views, skip_docs):
        if not subdir:
            return
        subdirlist = list()
        viewdirs = list()
        indexdirs = list()
        for item in os.listdir(subdir):
            path = os.path.join(subdir, item)
            if os.path.isfile(path):
                if (not skip_views and path.find("/design_docs") > 0) or \
                   (not skip_docs and path.find("/docs") > 0):
                    file_candidate.append(path)
            else:
                if item.find("/design_docs") > 0:
                        viewdirs.append(path)
                else:
                    subdirlist.append(path)
        for dir in subdirlist:
            JSONSource.enumerate_files(dir, file_candidate, skip_views, skip_docs)
        for dir in viewdirs:
            JSONSource.enumerate_files(dir, file_candidate, skip_views, skip_docs)

    def build_batch(self, batch, is_data, batch_max_size):
        for path in self.file_iter:
            if os.path.isfile(path):
                try:
                    fp = open(path, 'r')
                    dockey = JSONSource.gen_dockey(os.path.basename(path))
                    self.save_doc(batch, dockey, fp, is_data)
                    fp.close()
                except IOError, error:
                    logging.error("Fail to load json file with error" + str(error))
                if batch.size() >= batch_max_size:
                    return batch

        #at end of enumeration, clean up working directory
        self.done = True
        if self.working_dir:
            shutil.rmtree(self.working_dir)
        return batch

    @staticmethod
    def provide_design(opts, source_spec, source_bucket, source_map):
        f = source_spec.replace(JSON_SCHEME, "")
        files = list()
        working_dir = None
        if os.path.isfile(f) and f.endswith(".zip"):
            zfobj = zipfile.ZipFile(f)
            working_dir = tempfile.mkdtemp()
            ZipUtil(zfobj).extractall(working_dir)
            JSONSource.enumerate_files(working_dir, files, False, True)
        elif os.path.isdir(f):
            JSONSource.enumerate_files(working_dir, files, False, True)

        design_files = []
        for path in files:
            if os.path.isfile(path):
                f = open(path, 'r')
                design_files.append(f.read())
                f.close()

        if working_dir:
            shutil.rmtree(working_dir)

        return 0, design_files

    def provide_batch(self):
        if self.done:
            return 0, None

        batch = pump.Batch(self)
        if self.file_iter == None:
            self.f = self.spec.replace(JSON_SCHEME, "")
            files = list()
            if os.path.isfile(self.f) and self.f.endswith(".zip"):
                zfobj = zipfile.ZipFile(self.f)
                self.working_dir = tempfile.mkdtemp()
                ZipUtil(zfobj).extractall(self.working_dir)
                JSONSource.enumerate_files(self.working_dir, files, True, False)
            elif os.path.isdir(self.f):
                JSONSource.enumerate_files(self.f, files, True, False)
            else:
                try:
                    fp = open(self.f, 'r')
                    dockey = JSONSource.gen_dockey(os.path.basename(self.f))
                    self.save_doc(batch, dockey, fp, True)
                    fp.close()
                except IOError, error:
                    return "error: could not open json: %s; exception: %s" % \
                        (self.f, e), None
                self.done = True
                return 0,batch
            if len(files) > 0:
                self.file_iter = iter(files)

        if self.file_iter:
            return 0, self.build_batch(batch, True, self.opts.extra['batch_max_size'])

        return 0, None
