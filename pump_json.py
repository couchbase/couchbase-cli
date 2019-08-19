#!/usr/bin/env python3

import logging
import os
import json
import struct
import sys
import shutil
import tempfile
import zipfile
from typing import Tuple, Any, List, Optional, Dict
import couchbaseConstants
import pump

JSON_SCHEME = "json://"


class JSONSource(pump.Source):
    """Reads json file or directory or zip file that contains json files."""

    def __init__(self, opts, spec, source_bucket, source_node,
                 source_map, sink_map, ctl, cur):
        super(JSONSource, self).__init__(opts, spec, source_bucket, source_node,
                                         source_map, sink_map, ctl, cur)
        self.done: bool = False
        self.docs = list()
        self.file_iter = None

    @staticmethod
    def can_handle(opts, spec: str) -> bool:
        return spec.startswith(JSON_SCHEME) and \
            (os.path.isfile(spec.replace(JSON_SCHEME, "")) or \
             os.path.isdir(spec.replace(JSON_SCHEME, "")) or \
             spec.endswith(".zip"))

    @staticmethod
    def check(opts, spec: str) -> Tuple[couchbaseConstants.PUMP_ERROR, Dict[str, Any]]:
        return 0, {'spec': spec,
                   'buckets': [{'name': os.path.normpath(os.path.basename(spec)),
                                'nodes': [{'hostname': 'N/A'}]}]}

    def save_doc(self, batch: pump.Batch, dockey: bytes, docvalue: bytes):
        cmd = couchbaseConstants.CMD_TAP_MUTATION
        vbucket_id = 0x0000ffff
        # common flags:       0x02000000 (JSON)
        # legacy flags:       0x00000006 (JSON)
        cas, exp, flg = 0, 0, 0x02000006
        try:
            doc = json.loads(docvalue)
            if '_id' not in doc:
                msg = (cmd, vbucket_id, dockey, flg, exp, cas, b'', docvalue, 0, 0, 0, 0)
                batch.append(msg, len(docvalue))
            else:
                id = doc['_id'].encode('UTF-8')
                del doc['_id']
                docdata = {"doc": {
                    "json": doc,
                    "meta": {"id": id}
                }}
        except ValueError as error:
            logging.error(f'Fail to read json file with error: {error!s}')

    @staticmethod
    def gen_dockey(filename: str) -> str:
        return os.path.splitext(os.path.basename(filename))[0]

    @staticmethod
    def enumerate_files(subdir: str, file_candidate: List[str], skip_views: bool, skip_docs: bool):
        for item in os.listdir(subdir):
            path = os.path.join(subdir, item)
            if os.path.isfile(path):
                if (not skip_views and "design_docs" in path.split(os.path.sep)) or \
                   (not skip_docs and "docs" in path.split(os.path.sep)):
                    file_candidate.append(path)
            else:
                if not ((skip_docs and "docs" in path.split(os.path.sep)) or \
                   (skip_views and "design_docs" in path.split(os.path.sep))):
                    JSONSource.enumerate_files(path, file_candidate, skip_views, skip_docs)

    @staticmethod
    def provide_design(opts, source_spec: str, source_bucket, source_map) -> Tuple[couchbaseConstants.PUMP_ERROR,
                                                                                   List[bytes]]:
        design_files = list()
        f = source_spec.replace(JSON_SCHEME, "")

        if os.path.isfile(f) and f.endswith(".zip"):
            zf = zipfile.ZipFile(f)
            for path in zf.namelist():
                file = os.path.basename(path)
                # Skip the design_docs directory listing
                if file == "design_docs":
                    continue

                dir = os.path.basename(os.path.dirname(path))
                # Skip all files not in the design docs directory
                if dir != "design_docs":
                    continue

                design_files.append(zf.read(path))
            zf.close()
        elif os.path.isdir(f):
            files: List[str] = list()
            JSONSource.enumerate_files(f, files, False, True)
            for path in files:
                if os.path.isfile(path):
                    f = open(path, 'rb')  # type: ignore
                    design_files.append(f.read())  # type: ignore
                    f.close()  # type: ignore

        return 0, design_files

    def provide_batch(self) -> Tuple[couchbaseConstants.PUMP_ERROR, Optional[pump.Batch]]:
        if self.done:
            return 0, None

        # During the first iteration load the file names, this is only run once
        if not self.docs:
            self.prepare_docs()

        batch = pump.Batch(self)
        f = self.spec.replace(JSON_SCHEME, "")
        batch_max_size = self.opts.extra['batch_max_size']

        # Each iteration should return a batch or mark the loading a finished
        if os.path.isfile(f) and f.endswith(".zip"):
            zf = zipfile.ZipFile(f)
            while batch.size() < batch_max_size and self.docs:
                path = self.docs.pop()
                key = os.path.basename(path)
                if key.endswith('.json'):
                    key = key[:-5]
                value = zf.read(path)
                self.save_doc(batch, key, value)
            zf.close()
        else:
            while batch.size() < batch_max_size and self.docs:
                path = self.docs.pop()
                key = os.path.basename(path)
                if key.endswith('.json'):
                    key = key[:-5]
                try:
                    fp = open(path, 'rb')
                    value = fp.read()
                    fp.close()
                    self.save_doc(batch, key.encode(), value)
                except IOError as error:
                    logging.error(f'Fail to load json file with error: {error!s}')

        if not self.docs:
            self.done = True

        return 0, batch

    def prepare_docs(self):
        f = self.spec.replace(JSON_SCHEME, "")
        root_name = os.path.basename(f).split('.')[0]
        if os.path.isfile(f) and f.endswith(".zip"):
            zf = zipfile.ZipFile(f)
            for path in zf.namelist():
                file = os.path.basename(path)
                # Skip the docs directory listing
                if file == "docs":
                    continue

                dir = os.path.basename(os.path.dirname(path))

                # This condition is not allowed by the spec, but we allowing it
                # because the training team did properly follow the spec and we
                # don't want to break their training material. Since this tool
                # will be deprecated soon we are making an exception and
                # allowing this.
                if dir == "" or dir == root_name:
                    self.docs.append(path)
                    continue

                # Skip all files not in the docs directory
                # Note that we use a forward slash for all operating systems
                # because this is a zip file and zip paths always use a forward
                # slash
                if "docs" not in path.split("/"):
                    continue

                self.docs.append(path)
            zf.close()
        elif os.path.isdir(f):
            JSONSource.enumerate_files(f, self.docs, True, False)
        else:
            self.docs.append(f)
