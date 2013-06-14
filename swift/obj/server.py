# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Object Server for Swift """

from __future__ import with_statement
import cPickle as pickle
import errno
import os
import time
import traceback
from datetime import datetime
from hashlib import md5
from tempfile import mkstemp
from urllib import unquote
from contextlib import contextmanager

from xattr import getxattr, setxattr
from eventlet import sleep, Timeout, tpool

from swift.common.utils import mkdirs, normalize_timestamp, public, \
    storage_directory, hash_path, renamer, fallocate, fsync, fdatasync, \
    split_path, drop_buffer_cache, get_logger, write_pickle, \
    config_true_value, validate_device_partition, timing_stats
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_object_creation, check_mount, \
    check_float, check_utf8
from swift.common.exceptions import ConnectionTimeout, DiskFileError, \
    DiskFileNotExist, DiskFileCollision
from swift.obj.replicator import tpool_reraise, invalidate_hash, \
    quarantine_renamer, get_hashes
from swift.common.http import is_success
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPCreated, \
    HTTPInternalServerError, HTTPNoContent, HTTPNotFound, HTTPNotModified, \
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity, \
    HTTPClientDisconnect, HTTPMethodNotAllowed, Request, Response, UTC, \
    HTTPInsufficientStorage, HTTPForbidden, multi_range_iterator

# Ethan's Code here
from BitTorrent import reset_stderr
reset_stderr()
from BitTorrent.makemetafile import make_meta_files, return_make_meta_files
import threading
from BTL.bencode import bencode, bdecode

# imports for seeder
from __future__ import division

from BitTorrent.translation import _
import sys
import os
from cStringIO import StringIO
import logging
from logging import ERROR, WARNING
from time import strftime, sleep
import traceback
import BTL.stackthreading as threading
from BTL.platform import decode_from_filesystem, encode_for_filesystem
from BitTorrent.platform import get_dot_dir
from BTL.defer import DeferredEvent
from BitTorrent import inject_main_logfile
from BitTorrent.MultiTorrent import Feedback, MultiTorrent
from BitTorrent.defaultargs import get_defaults
from BitTorrent.parseargs import printHelp
from BitTorrent.prefs import Preferences
from BitTorrent import configfile
from BitTorrent import BTFailure, UserFailure
from BitTorrent import version
from BitTorrent import GetTorrent
from BTL.ConvertedMetainfo import ConvertedMetainfo
from BitTorrent.MultiTorrent import TorrentNotInitialized
from BitTorrent.RawServer_twisted import RawServer
from twisted.internet import task
from BitTorrent.UI import Size, Duration
inject_main_logfile()
from BitTorrent import console
from BitTorrent import stderr_console

TORRENTS_REQUEST_SUFFIX = '?torrent'
# Ethan's Code end


DATADIR = 'objects'
ASYNCDIR = 'async_pending'
PICKLE_PROTOCOL = 2
METADATA_KEY = 'user.swift.metadata'
MAX_OBJECT_NAME_LENGTH = 1024
# keep these lower-case
DISALLOWED_HEADERS = set('content-length content-type deleted etag'.split())

# Ethan's code - seeder thread

class SeederThread (threading.Thread):
    def __init__(self, ip, save_as, torrent_data):
        threading.Thread.__init__(self)
        uiname = 'bittorrent-console'
        defaults = get_defaults(uiname)
        data_dir = [[name, value,doc] for (name, value, doc) in defaults
                        if name == "data_dir"][0]
        defaults = [(name, value,doc) for (name, value, doc) in defaults
                        if not name == "data_dir"]
        ddir = os.path.join(get_dot_dir(), "console" )
        data_dir[1] = decode_from_filesystem(ddir)
        defaults.append( tuple(data_dir) )
        input_args = ['--ip', ip, '--save_as', save_as]
        config, args = configfile.parse_configuration_and_args(defaults,
                                       uiname, input_args, 0, 1)
        metainfo = GetTorrent.get_from_data(torrent_data)
        self.app = TorrentApp(metainfo, config)
    def run(self):
        self.app.run()

def wrap_log(context_string, logger):
    """Useful when passing a logger to a deferred's errback.  The context
       specifies what was being done when the exception was raised."""
    return lambda e, *args, **kwargs : logger.error(context_string, exc_info=e)


def fmttime(n):
    if n == 0:
        return _("download complete!")
    return _("finishing in %s") % (str(Duration(n)))

def fmtsize(n):
    s = str(n)
    size = s[-3:]
    while len(s) > 3:
        s = s[:-3]
        size = '%s,%s' % (s[-3:], size)
    size = '%s (%s)' % (size, str(Size(n)))
    return size


class HeadlessDisplayer(object):

    def __init__(self):
        self.done = False
        self.percentDone = ''
        self.timeEst = ''
        self.downRate = '---'
        self.upRate = '---'
        self.shareRating = ''
        self.seedStatus = ''
        self.peerStatus = ''
        self.errors = []
        self.file = ''
        self.downloadTo = ''
        self.fileSize = ''
        self.numpieces = 0

    def set_torrent_values(self, name, path, size, numpieces):
        self.file = name
        self.downloadTo = path
        self.fileSize = fmtsize(size)
        self.numpieces = numpieces

    def finished(self):
        self.done = True
        self.downRate = '---'
        self.display({'activity':_("download succeeded"), 'fractionDone':1})

    def error(self, errormsg):
        newerrmsg = strftime('[%H:%M:%S] ') + errormsg
        self.errors.append(newerrmsg)
        print errormsg
        #self.display({})    # display is only called periodically.

    def display(self, statistics):
        fractionDone = statistics.get('fractionDone')
        activity = statistics.get('activity')
        timeEst = statistics.get('timeEst')
        downRate = statistics.get('downRate')
        upRate = statistics.get('upRate')
        spew = statistics.get('spew')

        print '\n\n\n\n'
        if spew is not None:
            self.print_spew(spew)

        if timeEst is not None:
            self.timeEst = fmttime(timeEst)
        elif activity is not None:
            self.timeEst = activity

        if fractionDone is not None:
            self.percentDone = str(int(fractionDone * 1000) / 10)
        if downRate is not None:
            self.downRate = '%.1f KB/s' % (downRate / (1 << 10))
        if upRate is not None:
            self.upRate = '%.1f KB/s' % (upRate / (1 << 10))
        downTotal = statistics.get('downTotal')
        if downTotal is not None:
            upTotal = statistics['upTotal']
            if downTotal <= upTotal / 100:
                self.shareRating = _("oo  (%.1f MB up / %.1f MB down)") % (
                    upTotal / (1<<20), downTotal / (1<<20))
            else:
                self.shareRating = _("%.3f  (%.1f MB up / %.1f MB down)") % (
                   upTotal / downTotal, upTotal / (1<<20), downTotal / (1<<20))
            #numCopies = statistics['numCopies']
            #nextCopies = ', '.join(["%d:%.1f%%" % (a,int(b*1000)/10) for a,b in
            #        zip(xrange(numCopies+1, 1000), statistics['numCopyList'])])
            if not self.done:
                self.seedStatus = _("%d seen now") % statistics['numSeeds']
            #    self.seedStatus = _("%d seen now, plus %d distributed copies"
            #                        "(%s)") % (statistics['numSeeds' ],
            #                                   statistics['numCopies'],
            #                                   nextCopies)
            else:
                self.seedStatus = ""
            #    self.seedStatus = _("%d distributed copies (next: %s)") % (
            #        statistics['numCopies'], nextCopies)
            self.peerStatus = _("%d seen now") % statistics['numPeers']

        if not self.errors:
            print _("Log: none")
        else:
            print _("Log:")
        for err in self.errors[-4:]:
            print err 
        print    
        print _("saving:        "), self.file
        print _("file size:     "), self.fileSize
        print _("percent done:  "), self.percentDone
        print _("time left:     "), self.timeEst
        print _("download to:   "), self.downloadTo
        print _("download rate: "), self.downRate
        print _("upload rate:   "), self.upRate
        print _("share rating:  "), self.shareRating
        print _("seed status:   "), self.seedStatus
        print _("peer status:   "), self.peerStatus

    def print_spew(self, spew):
        s = StringIO()
        s.write('\n\n\n')
        for c in spew:
            s.write('%20s ' % c['ip'])
            if c['initiation'] == 'L':
                s.write('l')
            else:
                s.write('r')
            total, rate, interested, choked = c['upload']
            s.write(' %10s %10s ' % (str(int(total/10485.76)/100),
                                     str(int(rate))))
            if c['is_optimistic_unchoke']:
                s.write('*')
            else:
                s.write(' ')
            if interested:
                s.write('i')
            else:
                s.write(' ')
            if choked:
                s.write('c')
            else:
                s.write(' ')

            total, rate, interested, choked, snubbed = c['download']
            s.write(' %10s %10s ' % (str(int(total/10485.76)/100),
                                     str(int(rate))))
            if interested:
                s.write('i')
            else:
                s.write(' ')
            if choked:
                s.write('c')
            else:
                s.write(' ')
            if snubbed:
                s.write('s')
            else:
                s.write(' ')
            s.write('\n')
        print s.getvalue()


#class TorrentApp(Feedback):
class TorrentApp(object):

    class LogHandler(logging.Handler):
        def __init__(self, app, level=logging.NOTSET):
            logging.Handler.__init__(self,level)
            self.app = app
      
        def emit(self, record):
            self.app.display_error(record.getMessage() ) 
            if record.exc_info is not None:
                self.app.display_error( " %s: %s" % 
                    ( str(record.exc_info[0]), str(record.exc_info[1])))
                tb = record.exc_info[2]
                stack = traceback.extract_tb(tb)
                l = traceback.format_list(stack)
                for s in l:
                    self.app.display_error( " %s" % s )

    class LogFilter(logging.Filter):
        def filter( self, record):
            if record.name == "NatTraversal":
                return 0
            return 1  # allow.

    def __init__(self, metainfo, config):
        assert isinstance(metainfo, ConvertedMetainfo )
        self.metainfo = metainfo
        self.config = Preferences().initWithDict(config)
        self.torrent = None
        self.multitorrent = None
        self.logger = logging.getLogger("bittorrent-console")
        log_handler = TorrentApp.LogHandler(self)
        log_handler.setLevel(WARNING)
        logger = logging.getLogger()
        logger.addHandler(log_handler)

        # disable stdout and stderr error reporting to stderr.
        global stderr_console
        logging.getLogger('').removeHandler(console)
        if stderr_console is not None:
            logging.getLogger('').removeHandler(stderr_console)
        logging.getLogger().setLevel(WARNING)

    def start_torrent(self,metainfo,save_incomplete_as,save_as):
        """Tells the MultiTorrent to begin downloading."""
        try:
            self.d.display({'activity':_("initializing"), 
                               'fractionDone':0})
            multitorrent = self.multitorrent
            df = multitorrent.create_torrent(metainfo, save_incomplete_as,
                                             save_as)
            df.addErrback( wrap_log('Failed to start torrent', self.logger))
            def create_finished(torrent):
                self.torrent = torrent
                if self.torrent.is_initialized():
                   multitorrent.start_torrent(self.torrent.infohash)
                else:
                    # HEREDAVE: why should this set the doneflag?
                   self.core_doneflag.set()  # e.g., if already downloading...
            df.addCallback( create_finished )
        except KeyboardInterrupt:
            raise
        except UserFailure, e:
            self.logger.error( "Failed to create torrent: " + unicode(e.args[0]) )
        except Exception, e:
            self.logger.error( "Failed to create torrent", exc_info = e )
            return
        
    def run(self):
        self.core_doneflag = DeferredEvent()
        rawserver = RawServer(self.config)
        self.d = HeadlessDisplayer()

        # set up shut-down procedure before we begin doing things that
        # can throw exceptions.
        def shutdown():
            print "shutdown."
            self.d.display({'activity':_("shutting down"), 
                            'fractionDone':0})
            if self.multitorrent:
                df = self.multitorrent.shutdown()
                stop_rawserver = lambda *a : rawserver.stop()
                df.addCallbacks(stop_rawserver, stop_rawserver)
            else:
                rawserver.stop()

        # It is safe to addCallback here, because there is only one thread,
        # but even if the code were multi-threaded, core_doneflag has not
        # been passed to anyone.  There is no chance of a race condition
        # between core_doneflag's callback and addCallback.
        self.core_doneflag.addCallback(
            lambda r: rawserver.external_add_task(0, shutdown))
        
        rawserver.install_sigint_handler(self.core_doneflag)


        # semantics for --save_in vs --save_as:
        #   save_in specifies the directory in which torrent is written.
        #      If the torrent is a batch torrent then the files in the batch
        #      go in save_in/metainfo.name_fs/.
        #   save_as specifies the filename for the torrent in the case of
        #      a non-batch torrent, and specifies the directory name
        #      in the case of a batch torrent.  Thus the files in a batch
        #      torrent go in save_as/.
        metainfo = self.metainfo
        torrent_name = metainfo.name_fs  # if batch then this contains
                                         # directory name.

        if config['save_as']:
            if config['save_in']:
                raise BTFailure(_("You cannot specify both --save_as and "
                                  "--save_in."))
            saveas,bad = encode_for_filesystem(config['save_as'])
            if bad:
                raise BTFailure(_("Invalid path encoding."))
            savein = os.path.dirname(os.path.abspath(saveas))
        elif config['save_in']:
            savein,bad = encode_for_filesystem(config['save_in'])
            if bad:
                raise BTFailure(_("Invalid path encoding."))
            saveas = os.path.join(savein,torrent_name)
        else:
            saveas = torrent_name
        if config['save_incomplete_in']:
            save_incomplete_in,bad = \
                encode_for_filesystem(config['save_incomplete_in'])
            if bad:
                raise BTFailure(_("Invalid path encoding."))
            save_incomplete_as = os.path.join(save_incomplete_in,torrent_name)
        else:
            save_incomplete_as = os.path.join(savein,torrent_name)
    
        data_dir,bad = encode_for_filesystem(config['data_dir'])
        if bad:
            raise BTFailure(_("Invalid path encoding."))

        try: 
            self.multitorrent = \
                MultiTorrent(self.config, rawserver, data_dir,
                             is_single_torrent = True,
                             resume_from_torrent_config = False)
                
            self.d.set_torrent_values(metainfo.name, os.path.abspath(saveas),
                                metainfo.total_bytes, len(metainfo.hashes))
            self.start_torrent(self.metainfo, save_incomplete_as, saveas)
        
            self.get_status()
        except UserFailure, e:
            self.logger.error( unicode(e.args[0]) )
            rawserver.add_task(0, self.core_doneflag.set)
        except Exception, e:
            self.logger.error( "", exc_info = e )
            rawserver.add_task(0, self.core_doneflag.set)
            
        # always make sure events get processed even if only for
        # shutting down.
        rawserver.listen_forever()

    def get_status(self):
        self.multitorrent.rawserver.add_task(self.config['display_interval'],
                                             self.get_status)
        if self.torrent is not None:
            status = self.torrent.get_status(self.config['spew'])
            self.d.display(status)

    def display_error(self, text):
        """Called by the logger via LogHandler to display error messages in the
           curses window."""
        self.d.error(text)



# Ethan's code end

def read_metadata(fd):
    """
    Helper function to read the pickled metadata from an object file.

    :param fd: file descriptor to load the metadata from

    :returns: dictionary of metadata
    """
    metadata = ''
    key = 0
    try:
        while True:
            metadata += getxattr(fd, '%s%s' % (METADATA_KEY, (key or '')))
            key += 1
    except IOError:
        pass
    return pickle.loads(metadata)


def write_metadata(fd, metadata):
    """
    Helper function to write pickled metadata for an object file.

    :param fd: file descriptor to write the metadata
    :param metadata: metadata to write
    """
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        setxattr(fd, '%s%s' % (METADATA_KEY, key or ''), metastr[:254])
        metastr = metastr[254:]
        key += 1


class DiskFile(object):
    """
    Manage object files on disk.

    :param path: path to devices on the node
    :param device: device name
    :param partition: partition on the device the object lives in
    :param account: account name for the object
    :param container: container name for the object
    :param obj: object name for the object
    :param keep_data_fp: if True, don't close the fp, otherwise close it
    :param disk_chunk_size: size of chunks on file reads
    :param iter_hook: called when __iter__ returns a chunk
    :raises DiskFileCollision: on md5 collision
    """

    def __init__(self, path, device, partition, account, container, obj,
                 logger, keep_data_fp=False, disk_chunk_size=65536,
                 iter_hook=None):
        self.disk_chunk_size = disk_chunk_size
        self.iter_hook = iter_hook
        self.name = '/' + '/'.join((account, container, obj))
        name_hash = hash_path(account, container, obj)
        self.datadir = os.path.join(
            path, device, storage_directory(DATADIR, partition, name_hash))
        self.device_path = os.path.join(path, device)
        self.tmpdir = os.path.join(path, device, 'tmp')
        self.tmppath = None
        self.logger = logger
        self.metadata = {}
        self.meta_file = None
        self.data_file = None
        self.fp = None
        self.iter_etag = None
        self.started_at_0 = False
        self.read_to_eof = False
        self.quarantined_dir = None
        self.keep_cache = False
        self.suppress_file_closing = False
        if not os.path.exists(self.datadir):
            return
        files = sorted(os.listdir(self.datadir), reverse=True)
        for file in files:
            if file.endswith('.ts'):
                self.data_file = self.meta_file = None
                self.metadata = {'deleted': True}
                return
            if file.endswith('.meta') and not self.meta_file:
                self.meta_file = os.path.join(self.datadir, file)
            if file.endswith('.data') and not self.data_file:
                self.data_file = os.path.join(self.datadir, file)
                break
        if not self.data_file:
            return
        self.fp = open(self.data_file, 'rb')
        self.metadata = read_metadata(self.fp)
        if not keep_data_fp:
            self.close(verify_file=False)
        if self.meta_file:
            with open(self.meta_file) as mfp:
                for key in self.metadata.keys():
                    if key.lower() not in DISALLOWED_HEADERS:
                        del self.metadata[key]
                self.metadata.update(read_metadata(mfp))
        if 'name' in self.metadata:
            if self.metadata['name'] != self.name:
                self.logger.error(_('Client path %(client)s does not match '
                                    'path stored in object metadata %(meta)s'),
                                  {'client': self.name,
                                   'meta': self.metadata['name']})
                raise DiskFileCollision('Client path does not match path '
                                        'stored in object metadata')

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            dropped_cache = 0
            read = 0
            self.started_at_0 = False
            self.read_to_eof = False
            if self.fp.tell() == 0:
                self.started_at_0 = True
                self.iter_etag = md5()
            while True:
                chunk = self.fp.read(self.disk_chunk_size)
                if chunk:
                    if self.iter_etag:
                        self.iter_etag.update(chunk)
                    read += len(chunk)
                    if read - dropped_cache > (1024 * 1024):
                        self.drop_cache(self.fp.fileno(), dropped_cache,
                                        read - dropped_cache)
                        dropped_cache = read
                    yield chunk
                    if self.iter_hook:
                        self.iter_hook()
                else:
                    self.read_to_eof = True
                    self.drop_cache(self.fp.fileno(), dropped_cache,
                                    read - dropped_cache)
                    break
        finally:
            if not self.suppress_file_closing:
                self.close()

    def app_iter_range(self, start, stop):
        """Returns an iterator over the data file for range (start, stop)"""
        if start or start == 0:
            self.fp.seek(start)
        if stop is not None:
            length = stop - start
        else:
            length = None
        for chunk in self:
            if length is not None:
                length -= len(chunk)
                if length < 0:
                    # Chop off the extra:
                    yield chunk[:length]
                    break
            yield chunk

    def app_iter_ranges(self, ranges, content_type, boundary, size):
        """Returns an iterator over the data file for a set of ranges"""
        if not ranges:
            yield ''
        else:
            try:
                self.suppress_file_closing = True
                for chunk in multi_range_iterator(
                        ranges, content_type, boundary, size,
                        self.app_iter_range):
                    yield chunk
            finally:
                self.suppress_file_closing = False
                self.close()

    def _handle_close_quarantine(self):
        """Check if file needs to be quarantined"""
        try:
            self.get_data_file_size()
        except DiskFileError:
            self.quarantine()
            return
        except DiskFileNotExist:
            return

        if self.iter_etag and self.started_at_0 and self.read_to_eof and \
                'ETag' in self.metadata and \
                self.iter_etag.hexdigest() != self.metadata.get('ETag'):
            self.quarantine()

    def close(self, verify_file=True):
        """
        Close the file. Will handle quarantining file if necessary.

        :param verify_file: Defaults to True. If false, will not check
                            file to see if it needs quarantining.
        """
        if self.fp:
            try:
                if verify_file:
                    self._handle_close_quarantine()
            except (Exception, Timeout), e:
                self.logger.error(_(
                    'ERROR DiskFile %(data_file)s in '
                    '%(data_dir)s close failure: %(exc)s : %(stack)'),
                    {'exc': e, 'stack': ''.join(traceback.format_stack()),
                     'data_file': self.data_file, 'data_dir': self.datadir})
            finally:
                self.fp.close()
                self.fp = None

    def is_deleted(self):
        """
        Check if the file is deleted.

        :returns: True if the file doesn't exist or has been flagged as
                  deleted.
        """
        return not self.data_file or 'deleted' in self.metadata

    def is_expired(self):
        """
        Check if the file is expired.

        :returns: True if the file has an X-Delete-At in the past
        """
        return ('X-Delete-At' in self.metadata and
                int(self.metadata['X-Delete-At']) <= time.time())

    @contextmanager
    def mkstemp(self):
        """Contextmanager to make a temporary file."""
        if not os.path.exists(self.tmpdir):
            mkdirs(self.tmpdir)
        fd, self.tmppath = mkstemp(dir=self.tmpdir)
        try:
            yield fd
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            tmppath, self.tmppath = self.tmppath, None
            try:
                os.unlink(tmppath)
            except OSError:
                pass

    def put(self, fd, metadata, extension='.data'):
        """
        Finalize writing the file on disk, and renames it from the temp file to
        the real location.  This should be called after the data has been
        written to the temp file.

        :param fd: file descriptor of the temp file
        :param metadata: dictionary of metadata to be written
        :param extension: extension to be used when making the file
        """
        assert self.tmppath is not None
        metadata['name'] = self.name
        timestamp = normalize_timestamp(metadata['X-Timestamp'])
        write_metadata(fd, metadata)
        if 'Content-Length' in metadata:
            self.drop_cache(fd, 0, int(metadata['Content-Length']))
        tpool.execute(fsync, fd)
        invalidate_hash(os.path.dirname(self.datadir))
        renamer(self.tmppath,
                os.path.join(self.datadir, timestamp + extension))
        self.metadata = metadata

    def put_metadata(self, metadata, tombstone=False):
        """
        Short hand for putting metadata to .meta and .ts files.

        :param metadata: dictionary of metadata to be written
        :param tombstone: whether or not we are writing a tombstone
        """
        extension = '.ts' if tombstone else '.meta'
        with self.mkstemp() as fd:
            self.put(fd, metadata, extension=extension)

    def unlinkold(self, timestamp):
        """
        Remove any older versions of the object file.  Any file that has an
        older timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        timestamp = normalize_timestamp(timestamp)
        for fname in os.listdir(self.datadir):
            if fname < timestamp:
                try:
                    os.unlink(os.path.join(self.datadir, fname))
                except OSError, err:    # pragma: no cover
                    if err.errno != errno.ENOENT:
                        raise

    def drop_cache(self, fd, offset, length):
        """Method for no-oping buffer cache drop method."""
        if not self.keep_cache:
            drop_buffer_cache(fd, offset, length)

    def quarantine(self):
        """
        In the case that a file is corrupted, move it to a quarantined
        area to allow replication to fix it.

        :returns: if quarantine is successful, path to quarantined
                  directory otherwise None
        """
        if not (self.is_deleted() or self.quarantined_dir):
            self.quarantined_dir = quarantine_renamer(self.device_path,
                                                      self.data_file)
            self.logger.increment('quarantines')
            return self.quarantined_dir

    def get_data_file_size(self):
        """
        Returns the os.path.getsize for the file.  Raises an exception if this
        file does not match the Content-Length stored in the metadata. Or if
        self.data_file does not exist.

        :returns: file size as an int
        :raises DiskFileError: on file size mismatch.
        :raises DiskFileNotExist: on file not existing (including deleted)
        """
        try:
            file_size = 0
            if self.data_file:
                file_size = os.path.getsize(self.data_file)
                if 'Content-Length' in self.metadata:
                    metadata_size = int(self.metadata['Content-Length'])
                    if file_size != metadata_size:
                        raise DiskFileError(
                            'Content-Length of %s does not match file size '
                            'of %s' % (metadata_size, file_size))
                return file_size
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
        raise DiskFileNotExist('Data File does not exist.')


class ObjectController(object):
    """Implements the WSGI application for the Swift Object Server."""

    def __init__(self, conf):
        """
        Creates a new WSGI application for the Swift Object Server. An
        example configuration is given at
        <source-dir>/etc/object-server.conf-sample or
        /etc/swift/object-server.conf-sample.
        """
        self.logger = get_logger(conf, log_route='object-server')
        self.devices = conf.get('devices', '/srv/node/')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.node_timeout = int(conf.get('node_timeout', 3))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.keep_cache_size = int(conf.get('keep_cache_size', 5242880))
        self.keep_cache_private = \
            config_true_value(conf.get('keep_cache_private', 'false'))
        self.log_requests = config_true_value(conf.get('log_requests', 'true'))
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.slow = int(conf.get('slow', 0))
        self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024
        default_allowed_headers = '''
            content-disposition,
            content-encoding,
            x-delete-at,
            x-object-manifest,
            x-static-large-object,
        '''
        self.allowed_headers = set(
            i.strip().lower() for i in
            conf.get('allowed_headers', default_allowed_headers).split(',')
            if i.strip() and i.strip().lower() not in DISALLOWED_HEADERS)
        self.expiring_objects_account = \
            (conf.get('auto_create_account_prefix') or '.') + \
            'expiring_objects'
        self.expiring_objects_container_divisor = \
            int(conf.get('expiring_objects_container_divisor') or 86400)
        # Ethan's code - list of seeders
        self.seeders_list = []
        # Ethan's code end

    def async_update(self, op, account, container, obj, host, partition,
                     contdevice, headers_out, objdevice):
        """
        Sends or saves an async update.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param host: host that the container is on
        :param partition: partition that the container is on
        :param contdevice: device name that the container is on
        :param headers_out: dictionary of headers to send in the container
                            request
        :param objdevice: device name that the object is in
        """
        full_path = '/%s/%s/%s' % (account, container, obj)
        if all([host, partition, contdevice]):
            try:
                with ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(':', 1)
                    conn = http_connect(ip, port, contdevice, partition, op,
                                        full_path, headers_out)
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    response.read()
                    if is_success(response.status):
                        return
                    else:
                        self.logger.error(_(
                            'ERROR Container update failed '
                            '(saving for async update later): %(status)d '
                            'response from %(ip)s:%(port)s/%(dev)s'),
                            {'status': response.status, 'ip': ip, 'port': port,
                             'dev': contdevice})
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR container update failed with '
                    '%(ip)s:%(port)s/%(dev)s (saving for async update later)'),
                    {'ip': ip, 'port': port, 'dev': contdevice})
        async_dir = os.path.join(self.devices, objdevice, ASYNCDIR)
        ohash = hash_path(account, container, obj)
        self.logger.increment('async_pendings')
        write_pickle(
            {'op': op, 'account': account, 'container': container,
             'obj': obj, 'headers': headers_out},
            os.path.join(async_dir, ohash[-3:], ohash + '-' +
                         normalize_timestamp(headers_out['x-timestamp'])),
            os.path.join(self.devices, objdevice, 'tmp'))

    def container_update(self, op, account, container, obj, headers_in,
                         headers_out, objdevice):
        """
        Update the container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param headers_in: dictionary of headers from the original request
        :param headers_out: dictionary of headers to send in the container
                            request(s)
        :param objdevice: device name that the object is in
        """
        conthosts = [h.strip() for h in
                     headers_in.get('X-Container-Host', '').split(',')]
        contdevices = [d.strip() for d in
                       headers_in.get('X-Container-Device', '').split(',')]
        contpartition = headers_in.get('X-Container-Partition', '')

        if len(conthosts) != len(contdevices):
            # This shouldn't happen unless there's a bug in the proxy,
            # but if there is, we want to know about it.
            self.logger.error(_('ERROR Container update failed: different  '
                                'numbers of hosts and devices in request: '
                                '"%s" vs "%s"' %
                                (headers_in.get('X-Container-Host', ''),
                                 headers_in.get('X-Container-Device', ''))))
            return

        if contpartition:
            updates = zip(conthosts, contdevices)
        else:
            updates = []

        for conthost, contdevice in updates:
            self.async_update(op, account, container, obj, conthost,
                              contpartition, contdevice, headers_out,
                              objdevice)

    def delete_at_update(self, op, delete_at, account, container, obj,
                         headers_in, objdevice):
        """
        Update the expiring objects container when objects are updated.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param account: account name for the object
        :param container: container name for the object
        :param obj: object name
        :param headers_in: dictionary of headers from the original request
        :param objdevice: device name that the object is in
        """
        # Quick cap that will work from now until Sat Nov 20 17:46:39 2286
        # At that time, Swift will be so popular and pervasive I will have
        # created income for thousands of future programmers.
        delete_at = max(min(delete_at, 9999999999), 0)
        updates = [(None, None)]

        partition = None
        hosts = contdevices = [None]
        headers_out = {'x-timestamp': headers_in['x-timestamp'],
                       'x-trans-id': headers_in.get('x-trans-id', '-')}
        if op != 'DELETE':
            partition = headers_in.get('X-Delete-At-Partition', None)
            hosts = headers_in.get('X-Delete-At-Host', '')
            contdevices = headers_in.get('X-Delete-At-Device', '')
            updates = [upd for upd in
                       zip((h.strip() for h in hosts.split(',')),
                           (c.strip() for c in contdevices.split(',')))
                       if all(upd) and partition]
            if not updates:
                updates = [(None, None)]
            headers_out['x-size'] = '0'
            headers_out['x-content-type'] = 'text/plain'
            headers_out['x-etag'] = 'd41d8cd98f00b204e9800998ecf8427e'

        for host, contdevice in updates:
            self.async_update(
                op, self.expiring_objects_account,
                str(delete_at / self.expiring_objects_container_divisor *
                    self.expiring_objects_container_divisor),
                '%s-%s/%s/%s' % (delete_at, account, container, obj),
                host, partition, contdevice, headers_out, objdevice)

    @public
    @timing_stats()
    def POST(self, request):
        """Handle HTTP POST requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                                  content_type='text/plain')
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)

        if file.is_deleted() or file.is_expired():
            return HTTPNotFound(request=request)
        try:
            file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            file.quarantine()
            return HTTPNotFound(request=request)
        metadata = {'X-Timestamp': request.headers['x-timestamp']}
        metadata.update(val for val in request.headers.iteritems()
                        if val[0].lower().startswith('x-object-meta-'))
        for header_key in self.allowed_headers:
            if header_key in request.headers:
                header_caps = header_key.title()
                metadata[header_caps] = request.headers[header_key]
        old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
        if old_delete_at != new_delete_at:
            if new_delete_at:
                self.delete_at_update('PUT', new_delete_at, account, container,
                                      obj, request.headers, device)
            if old_delete_at:
                self.delete_at_update('DELETE', old_delete_at, account,
                                      container, obj, request.headers, device)
        file.put_metadata(metadata)
        return HTTPAccepted(request=request)

    @public
    @timing_stats()
    def PUT(self, request):
        """Handle HTTP PUT requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        error_response = check_object_creation(request, obj)
        if error_response:
            return error_response
        new_delete_at = int(request.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=request,
                                  content_type='text/plain')
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)
        orig_timestamp = file.metadata.get('X-Timestamp')
        upload_expiration = time.time() + self.max_upload_time
        etag = md5()
        upload_size = 0
        last_sync = 0
        elapsed_time = 0
        with file.mkstemp() as fd:
            try:
                fallocate(fd, int(request.headers.get('content-length', 0)))
            except OSError:
                return HTTPInsufficientStorage(drive=device, request=request)
            reader = request.environ['wsgi.input'].read
            for chunk in iter(lambda: reader(self.network_chunk_size), ''):
                start_time = time.time()
                upload_size += len(chunk)
                if time.time() > upload_expiration:
                    self.logger.increment('PUT.timeouts')
                    return HTTPRequestTimeout(request=request)
                etag.update(chunk)
                while chunk:
                    written = os.write(fd, chunk)
                    chunk = chunk[written:]
                # For large files sync every 512MB (by default) written
                if upload_size - last_sync >= self.bytes_per_sync:
                    tpool.execute(fdatasync, fd)
                    drop_buffer_cache(fd, last_sync, upload_size - last_sync)
                    last_sync = upload_size
                sleep()
                elapsed_time += time.time() - start_time

            if upload_size:
                self.logger.transfer_rate(
                    'PUT.' + device + '.timing', elapsed_time, upload_size)

            if 'content-length' in request.headers and \
                    int(request.headers['content-length']) != upload_size:
                return HTTPClientDisconnect(request=request)
            etag = etag.hexdigest()
            if 'etag' in request.headers and \
                    request.headers['etag'].lower() != etag:
                return HTTPUnprocessableEntity(request=request)
            metadata = {
                'X-Timestamp': request.headers['x-timestamp'],
                'Content-Type': request.headers['content-type'],
                'ETag': etag,
                'Content-Length': str(upload_size),
            }
            metadata.update(val for val in request.headers.iteritems()
                            if val[0].lower().startswith('x-object-meta-') and
                            len(val[0]) > 14)
            for header_key in self.allowed_headers:
                if header_key in request.headers:
                    header_caps = header_key.title()
                    metadata[header_caps] = request.headers[header_key]
            old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
            if old_delete_at != new_delete_at:
                if new_delete_at:
                    self.delete_at_update(
                        'PUT', new_delete_at, account, container, obj,
                        request.headers, device)
                if old_delete_at:
                    self.delete_at_update(
                        'DELETE', old_delete_at, account, container, obj,
                        request.headers, device)
            file.put(fd, metadata)
        file.unlinkold(metadata['X-Timestamp'])
        if not orig_timestamp or \
                orig_timestamp < request.headers['x-timestamp']:
            self.container_update(
                'PUT', account, container, obj, request.headers,
                {'x-size': file.metadata['Content-Length'],
                 'x-content-type': file.metadata['Content-Type'],
                 'x-timestamp': file.metadata['X-Timestamp'],
                 'x-etag': file.metadata['ETag'],
                 'x-trans-id': request.headers.get('x-trans-id', '-')},
                device)
        resp = HTTPCreated(request=request, etag=etag)
        return resp

    @public
    @timing_stats()
    def GET(self, request):
        """Handle HTTP GET requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, keep_data_fp=True,
                        disk_chunk_size=self.disk_chunk_size,
                        iter_hook=sleep)
        if file.is_deleted() or file.is_expired():
            if request.headers.get('if-match') == '*':
                return HTTPPreconditionFailed(request=request)
            else:
                return HTTPNotFound(request=request)
        try:
            file_size = file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            file.quarantine()
            return HTTPNotFound(request=request)
        if request.headers.get('if-match') not in (None, '*') and \
                file.metadata['ETag'] not in request.if_match:
            file.close()
            return HTTPPreconditionFailed(request=request)
        if request.headers.get('if-none-match') is not None:
            if file.metadata['ETag'] in request.if_none_match:
                resp = HTTPNotModified(request=request)
                resp.etag = file.metadata['ETag']
                file.close()
                return resp
        try:
            if_unmodified_since = request.if_unmodified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_unmodified_since and \
                datetime.fromtimestamp(
                    float(file.metadata['X-Timestamp']), UTC) > \
                if_unmodified_since:
            file.close()
            return HTTPPreconditionFailed(request=request)
        try:
            if_modified_since = request.if_modified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_modified_since and \
                datetime.fromtimestamp(
                    float(file.metadata['X-Timestamp']), UTC) < \
                if_modified_since:
            file.close()
            return HTTPNotModified(request=request)
        response = Response(app_iter=file,
                            request=request, conditional_response=True)
        
        # Ethan adding the torrent to the request
        ip = 'http://192.168.28.128:6969'
        ip_suffix = '/announce'
        save_as = file.data_file
        # response.headers['torrent'] = bencode(make_meta_files(ip, [save_as]))
        # response.headers['torrent_length'] = len(response.headers['torrent'])
        # response.body = bencode(make_meta_files(ip, [save_as]))
        # response.headers['content-length'] = len(response.headers['torrent'])
        
        # response.body = bencode(make_meta_files(ip, [file.data_file]))
        # self.seeder_thread = SeederThread(ip, save_as, bencode(make_meta_files(ip, [file.data_file])))
        # self.seeder_thread.start()
        # Ethan's Code end
        
        response.headers['Content-Type'] = file.metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in file.metadata.iteritems():
            if key.lower().startswith('x-object-meta-') or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = file.metadata['ETag']
        response.last_modified = float(file.metadata['X-Timestamp'])
        response.content_length = file_size
        if response.content_length < self.keep_cache_size and \
                (self.keep_cache_private or
                 ('X-Auth-Token' not in request.headers and
                  'X-Storage-Token' not in request.headers)):
            file.keep_cache = True
        if 'Content-Encoding' in file.metadata:
            response.content_encoding = file.metadata['Content-Encoding']
        response.headers['X-Timestamp'] = file.metadata['X-Timestamp']
        
        # Ethan Chaging the code
        # return request.get_response(response) # The real code
        res = request.get_response(response)
        print 'Ethan in obj Server GET. the path_qs is ' + request.path_qs
        if request.path_qs.endswith(TORRENTS_REQUEST_SUFFIX):
            # if res.status == 200:
            print 'Ethan in obj Server GET. this is a torrent request'
                # good torrent request
            # res.headers['torrent'] = bencode(make_meta_files(ip, [save_as]))
            # res.headers['torrent_length'] = len(res.headers['torrent'])
            # make_meta_files(ip, [save_as])
            # f = open(save_as + '.torrent', 'r')
            # res.app_iter = f.read()
            # f.close()
            res.app_iter = bencode(return_make_meta_files(ip + ip_suffix, [save_as]))
            res.headers['x-object-meta-orig-filename'] = res.headers['x-object-meta-orig-filename'] + '.torrent'
            seeder = SeederThread(ip, save_as, bencode(return_make_meta_files(ip + ip_suffix, [save_as])))
            self.seeders_list.append(seeder)
            seeder.start()
            # res.headers['Content-Length'] = len(bencode(make_meta_files(ip, [save_as])))
                # newRes = Response()
                # newRes.app_iter = bencode(make_meta_files(ip, [save_as]))
                # newRes.headers['Content-Length'] = len(bencode(make_meta_files(ip, [save_as])))
            return res
        return res
        # Ethan's code end

    @public
    @timing_stats(sample_rate=0.8)
    def HEAD(self, request):
        """Handle HTTP HEAD requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, err:
            resp = HTTPBadRequest(request=request)
            resp.content_type = 'text/plain'
            resp.body = str(err)
            return resp
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)
        if file.is_deleted() or file.is_expired():
            return HTTPNotFound(request=request)
        try:
            file_size = file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            file.quarantine()
            return HTTPNotFound(request=request)
        response = Response(request=request, conditional_response=True)
        response.headers['Content-Type'] = file.metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in file.metadata.iteritems():
            if key.lower().startswith('x-object-meta-') or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = file.metadata['ETag']
        response.last_modified = float(file.metadata['X-Timestamp'])
        # Needed for container sync feature
        response.headers['X-Timestamp'] = file.metadata['X-Timestamp']
        response.content_length = file_size
        if 'Content-Encoding' in file.metadata:
            response.content_encoding = file.metadata['Content-Encoding']
        return response

    @public
    @timing_stats()
    def DELETE(self, request):
        """Handle HTTP DELETE requests for the Swift Object Server."""
        try:
            device, partition, account, container, obj = \
                split_path(unquote(request.path), 5, 5, True)
            validate_device_partition(device, partition)
        except ValueError, e:
            return HTTPBadRequest(body=str(e), request=request,
                                  content_type='text/plain')
        if 'x-timestamp' not in request.headers or \
                not check_float(request.headers['x-timestamp']):
            return HTTPBadRequest(body='Missing timestamp', request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        response_class = HTTPNoContent
        file = DiskFile(self.devices, device, partition, account, container,
                        obj, self.logger, disk_chunk_size=self.disk_chunk_size)
        if 'x-if-delete-at' in request.headers and \
                int(request.headers['x-if-delete-at']) != \
                int(file.metadata.get('X-Delete-At') or 0):
            return HTTPPreconditionFailed(
                request=request,
                body='X-If-Delete-At and X-Delete-At do not match')
        orig_timestamp = file.metadata.get('X-Timestamp')
        if file.is_deleted() or file.is_expired():
            response_class = HTTPNotFound
        metadata = {
            'X-Timestamp': request.headers['X-Timestamp'], 'deleted': True,
        }
        old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
        if old_delete_at:
            self.delete_at_update('DELETE', old_delete_at, account,
                                  container, obj, request.headers, device)
        file.put_metadata(metadata, tombstone=True)
        file.unlinkold(metadata['X-Timestamp'])
        if not orig_timestamp or \
                orig_timestamp < request.headers['x-timestamp']:
            self.container_update(
                'DELETE', account, container, obj, request.headers,
                {'x-timestamp': metadata['X-Timestamp'],
                 'x-trans-id': request.headers.get('x-trans-id', '-')},
                device)
        resp = response_class(request=request)
        return resp

    @public
    @timing_stats(sample_rate=0.1)
    def REPLICATE(self, request):
        """
        Handle REPLICATE requests for the Swift Object Server.  This is used
        by the object replicator to get hashes for directories.
        """
        try:
            device, partition, suffix = split_path(
                unquote(request.path), 2, 3, True)
            validate_device_partition(device, partition)
        except ValueError, e:
            return HTTPBadRequest(body=str(e), request=request,
                                  content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):
            return HTTPInsufficientStorage(drive=device, request=request)
        path = os.path.join(self.devices, device, DATADIR, partition)
        if not os.path.exists(path):
            mkdirs(path)
        suffixes = suffix.split('-') if suffix else []
        _junk, hashes = tpool_reraise(get_hashes, path, recalculate=suffixes)
        return Response(body=pickle.dumps(hashes))

    def __call__(self, env, start_response):
        """WSGI Application entry point for the Swift Object Server."""
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)

        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8 or contains NULL')
        else:
            try:
                # disallow methods which have not been marked 'public'
                try:
                    method = getattr(self, req.method)
                    getattr(method, 'publicly_accessible')
                except AttributeError:
                    res = HTTPMethodNotAllowed()
                else:
                    res = method(req)
            except DiskFileCollision:
                res = HTTPForbidden(request=req)
            except (Exception, Timeout):
                self.logger.exception(_(
                    'ERROR __call__ error with %(method)s'
                    ' %(path)s '), {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = time.time() - start_time
        if self.log_requests:
            log_line = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %.4f' % (
                req.remote_addr,
                time.strftime('%d/%b/%Y:%H:%M:%S +0000',
                              time.gmtime()),
                req.method, req.path, res.status.split()[0],
                res.content_length or '-', req.referer or '-',
                req.headers.get('x-trans-id', '-'),
                req.user_agent or '-',
                trans_time)
            if req.method == 'REPLICATE':
                self.logger.debug(log_line)
            else:
                self.logger.info(log_line)
        if req.method in ('PUT', 'DELETE'):
            slow = self.slow - trans_time
            if slow > 0:
                sleep(slow)
        return res(env, start_response)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
