#!/usr/bin/python

import datetime
import os
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import time

from optparse import OptionParser

class IsiCommand(object):
    '''Shell command objects represents and manages single shell command'''

    def __init__(self,
            cmdline,
            filename_log = None):
        '''
        Creates IsiCommand instance.
            cmdline: string, command line
            filename_log: stdout,stderr filename or None if no redirection required
        '''

        # subprocess.Popen to manage child process
        self.popen = None

        # basic shell command properties
        self.cmdline = cmdline
        self.filename_log = filename_log
        self.fd_log = None

    def start(self):
        '''Start shell command. Should be called only once'''
        assert self.popen is None

        if self.filename_log:
            self.fd_log = open(self.filename_log, "w")

        arglist = shlex.split(self.cmdline)
        self.popen = subprocess.Popen(
            arglist,
            stdout = self.fd_log,
            stderr = self.fd_log,
            shell = False)

    def stop(self):
        '''Terminates shell command'''
        self.popen.terminate()

        if self.fd_log:
            self.fd_log.close()

cmdlines = {
    "cluster_health":
    "isi statistics query current --nodes=all --stats=cluster.health,cluster.node.count.up,cluster.node.count.down,cluster.node.list.down --no-footer --degraded --interval=%d",

    "system":
    "isi statistics system --nodes=all --degraded --format=table --output time,node,cpu,smb,ftp,http,nfs,hdfs,total,netin,netout,diskin,diskout --interval=%d",

    "protocol_by_op":
    "isi statistics protocol --protocols=nfs --zero --long --output=TimeStamp,NumOps,In,InAvg,Out,OutAvg,TimeMin,TimeMax,TimeAvg,Op --totalby=Op --degraded --interval=%d",

    "protocol_by_node":
    "isi statistics protocol --protocols=nfs --zero --long --output=TimeStamp,In,Out,Op --totalby=node --degraded --interval=%d",

    "drives":
    "isi statistics drive --nodes=all --degraded --sort=timeinq --limit=30 --output time,drive,type,opsin,bytesin,opsout,bytesout,timeavg,timeinq,queued,busy --interval=%d",

    "client_connections":
    " isi statistics query current --nodes=all --stats=node.clientstats.active.nfs,node.clientstats.connected.nfs --degraded --interval=%d",

    "heat_per_node":
    "isi statistics heat --totalby=node,event --sort=node,event --degraded --interval=%d",

    "heat_all_nodes":
    "isi statistics heat --totalby=node,event --sort=event --degraded --interval=%d",

    "query_node":
    "isi statistics query current --nodes=all --stats=node.health,node.cpu.user.avg,node.cpu.sys.avg,node.cpu.idle.avg,node.memory.used,node.memory.free,node.disk.busy.avg --degraded --no-footer --interval=%d",

    "query_network":
    "isi statistics query current --nodes=all --stats=node.open.files,node.net.ext.bytes.in.rate,node.net.ext.bytes.out.rate --degraded --no-footer --interval=%d",
}

DEFAULT_LOG_DIR = "/var/crash"
DEFAULT_STAT_INTERVAL = 30

def main():
    # parse args
    parser = OptionParser()
    parser.add_option("-d", "--log-directory",
        type = "string", dest = "log_directory",
        default = None,
        help = "Log directory to create (should not exist).  Defaults to %s"%DEFAULT_LOG_DIR)
    parser.add_option("-o", "--output",
        type = "string", dest = "tar_filename",
        default = None,
        help = "Output tarball filename to create. Should not exist! [Required]")
    parser.add_option("-i", "--interval",
        type = "int", dest = "interval",
        default = DEFAULT_STAT_INTERVAL,
        help = "Interval (sec) between stat samples.  Defaults to: %d"%DEFAULT_STAT_INTERVAL)
    (options, args) = parser.parse_args()

    # tarball filename is required!
    if not options.tar_filename:
        print "Please, specify output tarball filename using -o option"
        parser.print_help()
        return

    # tarball filename should NOT exist
    if os.path.exists(options.tar_filename):
        print "Output file: '%s' should not exist!" % options.tar_filename
        parser.print_help()
        return

    # make sure we can create a file for tar-ball
    # it is bad if we know about in in the very end
    f = open(options.tar_filename, "w")
    f.close()

    # create log directory (if explicitly specified)
    if options.log_directory is not None:
        if os.path.exists(options.log_directory):
            print "Error. Specified log directory: '%s' should not exist!" % options.log_directory
            return
        os.mkdir(options.log_directory)

    # create log directory (if not specified)
    if options.log_directory is None:
        var_dir = DEFAULT_LOG_DIR
        assert os.path.exists(var_dir)
        prefix = "nfs-perf-" + str(datetime.date.today()) + "-"
        options.log_directory = tempfile.mkdtemp(dir = var_dir, prefix = prefix)

    print "Created temp directory for logs: %s" % options.log_directory

    # create command objects
    objects = {}
    for name,cmdline in cmdlines.iteritems():
        filename_log = options.log_directory + "/" + name + ".log"

        # prepare command line (apply interval!)
        cmdline_final = cmdline % options.interval

        obj = IsiCommand(cmdline_final, filename_log)
        objects[name] = obj

    # starting shell commands
    print "Starting commands..."
    for name,obj in objects.iteritems():
        print "\t* " + name
        obj.start()

    print "Now gathering statistics from cluster..."

    # waiting for user input
    s = ''
    while s != 'stop':
        s = raw_input("When you are done with Hadoop job type 'stop': ").strip().lower()

    # terminate commands
    print "Terminating commands..."
    for name,obj in objects.iteritems():
        obj.stop()

    # create tarball
    print "Creating tar-ball: %s" % options.tar_filename
    tar = tarfile.open(options.tar_filename, "w:gz")
    tar.add(options.log_directory)
    tar.close()

    print "Deleting temp directory..."
    shutil.rmtree(options.log_directory)

    print "Please, send the following file to EMC Isilon representative:\n%s" % options.tar_filename

# run the code
if __name__ == "__main__":
    main()

