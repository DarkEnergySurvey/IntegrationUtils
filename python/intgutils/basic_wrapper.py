#!/usr/bin/env python

# $Id: basic_wrapper.py 44199 2016-10-04 14:01:31Z friedel $
# $Rev:: 44199                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2016-10-04 09:01:31 #$:  # Date of last commit.

# pylint: disable=print-statement

"""
Contains definition of basic wrapper class
"""

import time
import os
import shlex
import sys
import subprocess
import traceback
import re
import errno
from collections import OrderedDict

import intgutils.intgdefs as intgdefs
import intgutils.intgmisc as intgmisc
import intgutils.replace_funcs as replfuncs
import despymisc.miscutils as miscutils
import despymisc.provdefs as provdefs

from intgutils.wcl import WCL


WRAPPER_OUTPUT_PREFIX = 'WRAP: '


class BasicWrapper(object):
    """ Basic wrapper class """

    ######################################################################
    def __init__(self, wclfile, debug=1):
        """ Read input wcl to initialize object """

        self.input_filename = wclfile
        self.inputwcl = WCL()
        with open(wclfile, 'r') as infh:
            self.inputwcl.read(infh)
        self.debug = debug

        # note: WGB handled by file registration using OW_OUTPUTS_BY_SECT
        provdict = OrderedDict({provdefs.PROV_USED: OrderedDict(),
                                provdefs.PROV_WDF: OrderedDict()})
        self.outputwcl = WCL({'wrapper': OrderedDict(),
                              intgdefs.OW_PROV_SECT: provdict,
                              intgdefs.OW_OUTPUTS_BY_SECT: {}})

        self.last_num_derived = 0
        self.last_num_meta = 0
        self.curr_task = []
        self.curr_exec = None

    ######################################################################
    def determine_status(self):
        """ Check all task status to determine wrapper status """
        status = 0

        execs = intgmisc.get_exec_sections(self.inputwcl, intgdefs.IW_EXEC_PREFIX)
        if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO:  exec sections = %s" % execs, WRAPPER_OUTPUT_PREFIX)

        for ekey in sorted(execs.keys()):
            if ekey in self.outputwcl:
                if 'task_info' in self.outputwcl[ekey]:
                    for taskd in self.outputwcl[ekey]['task_info'].values():
                        if 'status' in taskd:
                            if taskd['status'] != 0:
                                status = taskd['status']
                        else:
                            if miscutils.fwdebug_check(3, "BASICWRAP_DEBUG"):
                                miscutils.fwdebug_print("WARN: Missing status in outputwcl task_info for %s" % ekey,
                                                        WRAPPER_OUTPUT_PREFIX)
                            status = 1
                else:
                    if miscutils.fwdebug_check(3, "BASICWRAP_DEBUG"):
                        miscutils.fwdebug_print("WARN: Missing task_info in outputwcl for %s" % \
                                                ekey, WRAPPER_OUTPUT_PREFIX)
                    status = 1
            else:
                status = 1

        return status

    ######################################################################
    def get_status(self):
        """ Return status of wrapper execution """
        status = 1
        if 'status' in self.outputwcl['wrapper']:
            status = self.outputwcl['wrapper']['status']

        return status

    ######################################################################
    def check_command_line(self, exsect, exwcl):
        """ Ensure that certain command line arguments are specified """
        # pylint: disable=unused-argument

        self.start_exec_task('check_command_line')

        #if intgdefs.IW_CHECK_COMMAND in self.inputwcl and \
        #   miscutils.convertBool(self.inputwcl[intgdefs.IW_CHECK_COMMAND]):
        #
        #    if intgdefs.IW_EXEC_DEF in self.inputwcl:
        #        execdefs = self.inputwcl[intgdefs.IW_EXEC_DEF]
        #
        #        execsect = "%s_%s" % (intgdefs.IW_EXEC_PREFIX, execnum)
        #        if (execsect.lower() in execdefs and
        #                intgdefs.IW_CMD_REQ_ARGS in execdefs[execsect.lower()]):
        #            cmd_req_args = execdefs[execsect.lower()][intgdefs.IW_CMD_REQ_ARGS]
        #            req_args = miscutils.fwsplit(cmd_req_args, ',')
        #

        self.end_exec_task(0)

        return 0


    ######################################################################
    def create_command_line(self, execnum, exwcl):
        """ Create command line string handling hyphens appropriately"""
        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("execnum = '%s', exwcl = '%s'" % (execnum, exwcl),
                                    WRAPPER_OUTPUT_PREFIX)
        self.start_exec_task('create_command_line')

        cmdstr = ""
        if 'execname' in exwcl:
            cmdlist = [exwcl['execname']]

            if 'cmdline' in exwcl:
                posargs = {}  # save positional args to insert later

                hyphen_type = 'allsingle'
                if 'cmd_hyphen' in exwcl:
                    hyphen_type = exwcl['cmd_hyphen']

                # loop through command line args
                for key, val in exwcl['cmdline'].items():
                    if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                        miscutils.fwdebug_print("key = '%s', val = '%s'" % (key, val),
                                                WRAPPER_OUTPUT_PREFIX)

                    # replace any variables
                    expandval = replfuncs.replace_vars(val, self.inputwcl)[0]
                    if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                        miscutils.fwdebug_print("expandval = '%s'" % (expandval),
                                                WRAPPER_OUTPUT_PREFIX)

                    if key.startswith('_'):
                        patmatch = re.match(r'_(\d+)', key)
                        if patmatch:
                            posargs[patmatch.group(1)] = expandval  # save for later
                        else:
                            raise ValueError('Invalid positional argument name: %s' % key)
                    else:
                        hyphen = intgmisc.get_cmd_hyphen(hyphen_type, key)

                        if expandval == '_flag':
                            cmdlist.append(" %s%s" % (hyphen, key))
                        else:
                            cmdlist.append(" %s%s %s" % (hyphen, key, expandval))

                # insert position sensitive arguments into specified location in argument list
                for k in sorted(posargs.iterkeys()):
                    cmdlist.insert(int(k), "%s" % posargs[k])

            # convert list of args into string
            if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("cmdlist = '%s'" % (cmdlist), WRAPPER_OUTPUT_PREFIX)
            cmdstr = ' '.join(cmdlist)
        else:
            print "Error: missing execname in wcl for exec #%d" % execnum
            print "exec wcl = %s" % exwcl
            raise KeyError('Missing execname in wcl for exec #%d' % execnum)

        self.curr_exec['cmdline'] = cmdstr
        self.end_exec_task(0)


    ######################################################################
    def save_exec_version(self, exwcl):
        """ Run command with version flag and parse output for version information """
        # assumes exit code for version is 0

        self.start_exec_task('save_exec_version')

        ver = None

        execname = exwcl['execname']
        if 'version_flag' in exwcl and 'version_pattern' in exwcl:
            verflag = exwcl['version_flag']
            verpat = exwcl['version_pattern']

            cmd = "%s %s" % (execname, verflag)
            try:
                process = subprocess.Popen(shlex.split(cmd),
                                           shell=False,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT)
            except:
                (exc_type, exc_value) = sys.exc_info()[0:2]
                print "********************"
                print "Unexpected error: %s - %s" % (exc_type, exc_value)
                print "cmd> %s" % cmd
                print "Probably could not find %s in path" % shlex.split(cmd)[0]
                print "Check for mispelled execname in submit wcl or"
                print "    make sure that the corresponding eups package is in the metapackage "
                print "    and it sets up the path correctly"
                raise

            process.wait()
            out = process.communicate()[0]
            if process.returncode != 0:
                miscutils.fwdebug_print("INFO:  problem when running code to get version",
                                        WRAPPER_OUTPUT_PREFIX)
                miscutils.fwdebug_print("\t%s %s %s" % (execname, verflag, verpat),
                                        WRAPPER_OUTPUT_PREFIX)
                miscutils.fwdebug_print("\tcmd> %s" % cmd, WRAPPER_OUTPUT_PREFIX)
                miscutils.fwdebug_print("\t%s" % out, WRAPPER_OUTPUT_PREFIX)
                ver = None
            else:
                # parse output with verpat
                try:
                    vmatch = re.search(verpat, out)
                    if vmatch:
                        ver = vmatch.group(1)
                    else:
                        if miscutils.fwdebug_check(1, 'BASICWRAP_DEBUG'):
                            miscutils.fwdebug_print("re.search didn't find version for exec %s" % \
                                                    execname, WRAPPER_OUTPUT_PREFIX)
                        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                            miscutils.fwdebug_print("\tcmd output=%s" % out, WRAPPER_OUTPUT_PREFIX)
                            miscutils.fwdebug_print("\tcmd verpat=%s" % verpat,
                                                    WRAPPER_OUTPUT_PREFIX)
                except Exception as err:
                    #print type(err)
                    ver = None
                    print "Error: Exception from re.match.  Didn't find version: %s" % err
                    raise
        else:
            miscutils.fwdebug_print("INFO: Could not find version info for exec %s" % execname,
                                    WRAPPER_OUTPUT_PREFIX)
            ver = None

        if ver is not None:
            self.curr_exec['version'] = ver
        self.end_exec_task(0)

    ######################################################################
    def create_output_dirs(self, exwcl):
        """ Make directories for output files """

        self.start_exec_task('create_output_dirs')

        if intgdefs.IW_OUTPUTS in exwcl:
            for sect in miscutils.fwsplit(exwcl[intgdefs.IW_OUTPUTS]):
                sectkeys = sect.split('.')
                if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                    miscutils.fwdebug_print("INFO: sectkeys=%s" % sectkeys, WRAPPER_OUTPUT_PREFIX)
                if sectkeys[0] == intgdefs.IW_FILE_SECT:
                    sectname = sectkeys[1]
                    if sectname in self.inputwcl[intgdefs.IW_FILE_SECT]:
                        if 'fullname' in self.inputwcl[intgdefs.IW_FILE_SECT][sectname]:
                            fullnames = self.inputwcl[intgdefs.IW_FILE_SECT][sectname]['fullname']
                            if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                                miscutils.fwdebug_print("INFO: fullname = %s" % fullnames,
                                                        WRAPPER_OUTPUT_PREFIX)
                            if '$RNMLST{' in fullnames:
                                raise ValueError('Deprecated $RNMLST in output filename')
                            else:
                                for fname in miscutils.fwsplit(fullnames, ','):
                                    outdir = os.path.dirname(fname)
                                    miscutils.coremakedirs(outdir)
                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    (_, listsect, filesect) = sect.split('.')

                    ldict = self.inputwcl[intgdefs.IW_LIST_SECT][sectkeys[1]]

                    # check list itself exists
                    listname = ldict['fullname']
                    if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                        miscutils.fwdebug_print("\tINFO: Checking existence of '%s'" % listname,
                                                WRAPPER_OUTPUT_PREFIX)

                    if not os.path.exists(listname):
                        miscutils.fwdebug_print("\tError: list '%s' does not exist." % listname,
                                                WRAPPER_OUTPUT_PREFIX)
                        raise IOError("List not found: %s does not exist" % listname)

                    # get list format: space separated, csv, wcl, etc
                    listfmt = intgdefs.DEFAULT_LIST_FORMAT
                    if intgdefs.LIST_FORMAT in ldict:
                        listfmt = ldict[intgdefs.LIST_FORMAT]

                    # read fullnames from list file
                    fullnames = intgmisc.read_fullnames_from_listfile(listname, listfmt, ldict['columns'])
                    if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                        miscutils.fwdebug_print("\tINFO: fullnames=%s" % fullnames, WRAPPER_OUTPUT_PREFIX)

                    for fname in fullnames[filesect]:
                        outdir = os.path.dirname(fname)
                        miscutils.coremakedirs(outdir)

        self.end_exec_task(0)

    ######################################################################
    def run_exec(self):
        """ Run given command line """

        self.start_exec_task('run_exec')
        cmdline = self.curr_exec['cmdline']

        retcode = None
        procinfo = None

        miscutils.fwdebug_print("INFO: cmd = %s" % cmdline, WRAPPER_OUTPUT_PREFIX)
        print '*' * 70
        sys.stdout.flush()
        try:
            (retcode, procinfo) = intgmisc.run_exec(cmdline)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

            print "********************"
            (exc_type, exc_value, _) = sys.exc_info()
            print "%s - %s" % (exc_type, exc_value)
            print "cmd> %s" % cmdline
            print "Probably could not find %s in path" % cmdline.split()[0]
            print "Check for mispelled execname in submit wcl or"
            print "    make sure that the corresponding eups package is in "
            print "    the metapackage and it sets up the path correctly"
            raise

        sys.stdout.flush()

        if retcode != 0:
            if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("\tINFO: cmd exited with non-zero exit code = %s" % retcode,
                                        WRAPPER_OUTPUT_PREFIX)
                miscutils.fwdebug_print("\tINFO: failed cmd = %s" % cmdline, WRAPPER_OUTPUT_PREFIX)
        else:
            if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("\tINFO: cmd exited with exit code = 0",
                                        WRAPPER_OUTPUT_PREFIX)

        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("END", WRAPPER_OUTPUT_PREFIX)
        print '*' * 70
        self.curr_exec['status'] = retcode
        self.curr_exec['procinfo'] = procinfo

        self.end_exec_task(retcode)


    ######################################################################
    def check_inputs(self, ekey):
        """ Check which input files/lists do not exist """

        self.start_exec_task('check_inputs')

        existfiles = {}
        missingfiles = []

        ins, _ = intgmisc.get_fullnames(self.inputwcl, self.inputwcl, ekey)
        for sect in ins:
            exists, missing = intgmisc.check_files(ins[sect])
            existfiles[sect] = exists

            if len(missing) != 0:
                for mfile in missing:
                    miscutils.fwdebug_print("ERROR: input '%s' does not exist." % mfile,
                                            WRAPPER_OUTPUT_PREFIX)
                os.system("pwd")
                os.system("find . -type f")
                sys.exit(3)
                #raise IOError("At least one input file not found.")    # if missing inputs, just abort

        self.end_exec_task(0)
        return existfiles


    ######################################################################
    #def check_output_list(self, sect):
    #    """ Check that the output files described by a list file exist """
    #
    #    if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
    #        miscutils.fwdebug_print("INFO: Beg sect=%s" % sect, WRAPPER_OUTPUT_PREFIX)
    #
    #   _, existfiles, missingfiles = intgmisc.check_list(sect,
    #                                                     self.inputwcl[intgdefs.IW_LIST_SECT],
    #                                                     self.inputwcl[intgdefs.IW_FILE_SECT])
    #
    #   if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
    #       miscutils.fwdebug_print("INFO: existfiles=%s" % existfiles, WRAPPER_OUTPUT_PREFIX)
    #       miscutils.fwdebug_print("INFO: missingfiles=%s" % missingfiles, WRAPPER_OUTPUT_PREFIX)
    #   if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
    #       miscutils.fwdebug_print("INFO: end", WRAPPER_OUTPUT_PREFIX)
    #
    #   return (existfiles, missingfiles)


    ######################################################################
#    def check_output_files(self, sect):
#        """ Check that the files for a single output file section exist """
#
#        sectkeys = sect.split('.')
#        sectname = sectkeys[1]
#
#        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
#            miscutils.fwdebug_print("INFO: Beg sectname=%s" % sectname, WRAPPER_OUTPUT_PREFIX)
#        existfiles = []
#        missingfiles = []
#
#        if sectname in self.inputwcl[intgdefs.IW_FILE_SECT]:
#            if 'fullname' in self.inputwcl[intgdefs.IW_FILE_SECT][sectname]:
#                fnames = replfuncs.replace_vars(self.inputwcl[intgdefs.IW_FILE_SECT][sectname]['fullname'], self.inputwcl)[0]
#                #fnames = miscutils.fwsplit(self.inputwcl[intgdefs.IW_FILE_SECT][sectname]['fullname'], ',')
#                fnames = miscutils.fwsplit(fnames, ',')
#                if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
#                    miscutils.fwdebug_print("INFO: fullname = %s" % fnames, WRAPPER_OUTPUT_PREFIX)
#                for filen in fnames:
#                    if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
#                        miscutils.fwdebug_print("\tINFO: Checking existence of file '%s'" % filen,
#                                                WRAPPER_OUTPUT_PREFIX)
#                    if os.path.exists(filen) and os.path.getsize(filen) > 0:
#                        existfiles.append(filen)
#                    else:
#                        missingfiles.append(filen)
#
#        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
#            miscutils.fwdebug_print("INFO: existfiles=%s" % existfiles, WRAPPER_OUTPUT_PREFIX)
#            miscutils.fwdebug_print("INFO: missingfiles=%s" % missingfiles, WRAPPER_OUTPUT_PREFIX)
#        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
#            miscutils.fwdebug_print("INFO: end", WRAPPER_OUTPUT_PREFIX)
#
#        return (existfiles, missingfiles)


    ######################################################################
    def get_optout(self, sect):
        """ Return whether file(s) are optional outputs """

        optout = False
        sectkeys = sect.split('.')
        if sectkeys[0] == intgdefs.IW_FILE_SECT:
            if intgdefs.IW_OUTPUT_OPTIONAL in self.inputwcl.get(sect):
                optout = miscutils.convertBool(self.inputwcl.get(sect)[intgdefs.IW_OUTPUT_OPTIONAL])
        elif sectkeys[0] == intgdefs.IW_LIST_SECT:
            if intgdefs.IW_OUTPUT_OPTIONAL in self.inputwcl.get("%s.%s" % (intgdefs.IW_FILE_SECT, sectkeys[2])):
                optout = miscutils.convertBool(self.inputwcl.get("%s.%s" % (intgdefs.IW_FILE_SECT, sectkeys[2]))[intgdefs.IW_OUTPUT_OPTIONAL])
        else:
            raise KeyError("Unknown data section %s" % sectkeys[0])

        return optout

    ######################################################################
    def check_outputs(self, ekey, exitcode):
        """ Check which output files were created, renaming if necessary """

        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: Beg", WRAPPER_OUTPUT_PREFIX)

        self.start_exec_task('check_outputs')

        existfiles = {}
        missingfiles = {}

        _, outs = intgmisc.get_fullnames(self.inputwcl, self.inputwcl, ekey)
        for sect in outs:
            if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("INFO: sect=%s" % sect, WRAPPER_OUTPUT_PREFIX)

            exists, missing = intgmisc.check_files(outs[sect])
            existfiles.update({sect:exists})
            if len(missing) > 0:
                optout = self.get_optout(sect)
                if optout:
                    if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                        miscutils.fwdebug_print("\tINFO: optional output file '%s' does not exist (sect: %s)." % \
                                                (missing, sect), WRAPPER_OUTPUT_PREFIX)
                elif exitcode != 0:
                    if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                        miscutils.fwdebug_print("INFO: skipping missing output due to non-zero exit code (%s: %s)" % (sect, missing),
                                                WRAPPER_OUTPUT_PREFIX)
                else:
                    miscutils.fwdebug_print("ERROR: Missing required output file(s) (%s:%s)" % (sect, missing),
                                            WRAPPER_OUTPUT_PREFIX)
                    missingfiles.update({sect:missing})


        if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: existfiles=%s" % existfiles, WRAPPER_OUTPUT_PREFIX)
            miscutils.fwdebug_print("INFO: missingfiles=%s" % missingfiles, WRAPPER_OUTPUT_PREFIX)

        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: end", WRAPPER_OUTPUT_PREFIX)

        if len(missingfiles) > 0:
            status = 1
        else:
            status = 0
        self.end_exec_task(status)
        return existfiles

    ######################################################################
    def transform_inputs(self, exwcl):
        """ Transform inputs stored by DESDM into form needed by exec """
        # pylint: disable=unused-argument
        self.start_exec_task('transform_inputs')
        self.end_exec_task(0)

    ######################################################################
    def transform_outputs(self, exwcl):
        """ Transform outputs created by exec into form needed by DESDM """
        # pylint: disable=unused-argument
        self.start_exec_task('transform_outputs')
        self.end_exec_task(0)


    ######################################################################
    def save_provenance(self, execsect, exwcl, infiles, outfiles, exitcode):
        """ Create provenance wcl """
        self.start_exec_task('save_provenance')

        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: Beg", WRAPPER_OUTPUT_PREFIX)
        if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: infiles = %s" % infiles, WRAPPER_OUTPUT_PREFIX)
            miscutils.fwdebug_print("INFO: outfiles = %s" % outfiles, WRAPPER_OUTPUT_PREFIX)

        num_errs = 0

        # convert probably fullnames in outexist to filename+compression
        new_outfiles = OrderedDict()
        for exlabel, exlist in outfiles.items():
            if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("INFO: exlabel=%s exlist=%s" % (exlabel, exlist),
                                        WRAPPER_OUTPUT_PREFIX)
            newlist = []
            for fullname in exlist:
                basename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_BASENAME)
                newlist.append(basename)
            if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("INFO: newlist=%s" % (newlist), WRAPPER_OUTPUT_PREFIX)

            new_outfiles[exlabel] = newlist

        prov = self.outputwcl[intgdefs.OW_PROV_SECT]

        # used
        new_infiles = {}
        if len(infiles) > 0:
            all_infiles = []
            for key, sublist in infiles.items():
                new_infiles[key] = []
                for fullname in sublist:
                    basename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_BASENAME)
                    all_infiles.append(basename)
                    new_infiles[key].append(basename)
            prov[provdefs.PROV_USED][execsect] = provdefs.PROV_DELIM.join(all_infiles)

        # was_generated_by - done by PFW when saving metadata

        # was_derived_from
        if intgdefs.IW_DERIVATION in exwcl:
            wdf = prov[provdefs.PROV_WDF]
            derived_pairs = miscutils.fwsplit(exwcl[intgdefs.IW_DERIVATION], provdefs.PROV_DELIM)
            for dpair in derived_pairs:
                if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                    miscutils.fwdebug_print("INFO: dpair = %s" % dpair, WRAPPER_OUTPUT_PREFIX)
                (parent_sect, child_sect) = miscutils.fwsplit(dpair, ':')[:2]
                if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                    miscutils.fwdebug_print("INFO: parent_sect = %s" % parent_sect, WRAPPER_OUTPUT_PREFIX)
                    miscutils.fwdebug_print("INFO: child_sect = %s" % child_sect, WRAPPER_OUTPUT_PREFIX)

                optout = self.get_optout(child_sect)
                #parent_key = miscutils.fwsplit(parent_sect, '.')[-1]
                #child_key = miscutils.fwsplit(child_sect, '.')[-1]

                if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                    #miscutils.fwdebug_print("INFO: parent_key = %s" % parent_key,
                    #                        WRAPPER_OUTPUT_PREFIX)
                    #miscutils.fwdebug_print("INFO: child_key = %s" % child_key,
                    #                        WRAPPER_OUTPUT_PREFIX)
                    miscutils.fwdebug_print("INFO: optout = %s" % optout,
                                            WRAPPER_OUTPUT_PREFIX)
                    miscutils.fwdebug_print("INFO: new_outfiles.keys = %s" % new_outfiles.keys(),
                                            WRAPPER_OUTPUT_PREFIX)
                    miscutils.fwdebug_print("INFO: new_outfiles = %s" % new_outfiles,
                                            WRAPPER_OUTPUT_PREFIX)

                if child_sect not in new_outfiles or \
                        new_outfiles[child_sect] is None or \
                        len(new_outfiles[child_sect]) == 0:
                    if optout:
                        if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                            miscutils.fwdebug_print("INFO: skipping missing optional output %s:%s" % (parent_sect, child_sect),
                                                    WRAPPER_OUTPUT_PREFIX)
                    elif exitcode != 0:
                        if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                            miscutils.fwdebug_print("INFO: skipping missing output due to non-zero exit code %s:%s" % (parent_sect, child_sect),
                                                    WRAPPER_OUTPUT_PREFIX)
                    else:
                        miscutils.fwdebug_print("ERROR: Missing child output files in wdf tuple (%s:%s)" % (parent_sect, child_sect),
                                                WRAPPER_OUTPUT_PREFIX)
                        num_errs += 1
                else:
                    self.last_num_derived += 1
                    key = 'derived_%d' % self.last_num_derived
                    if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                        miscutils.fwdebug_print("INFO: key = %s" % key, WRAPPER_OUTPUT_PREFIX)
                        miscutils.fwdebug_print("INFO: before wdf = %s" % prov[provdefs.PROV_WDF],
                                                WRAPPER_OUTPUT_PREFIX)


                    if parent_sect not in infiles and parent_sect not in new_outfiles:
                        miscutils.fwdebug_print("parent_sect = %s" % parent_sect, WRAPPER_OUTPUT_PREFIX)
                        miscutils.fwdebug_print("infiles.keys() = %s" % infiles.keys(),
                                                WRAPPER_OUTPUT_PREFIX)
                        miscutils.fwdebug_print("outfiles.keys() = %s" % outfiles.keys(),
                                                WRAPPER_OUTPUT_PREFIX)
                        miscutils.fwdebug_print("used = %s" % exwcl[intgdefs.IW_INPUTS],
                                                WRAPPER_OUTPUT_PREFIX)
                        miscutils.fwdebug_print("ERROR: Could not find parent files for %s" % \
                                                (dpair), WRAPPER_OUTPUT_PREFIX)
                        num_errs += 1
                    else:
                        wdf[key] = OrderedDict()
                        wdf[key][provdefs.PROV_CHILDREN] = provdefs.PROV_DELIM.join(new_outfiles[child_sect])
                        if parent_sect in infiles:
                            wdf[key][provdefs.PROV_PARENTS] = provdefs.PROV_DELIM.join(new_infiles[parent_sect])
                        elif parent_sect in new_outfiles:
                            # this output was generated within same
                            #   program/wrapper from other output files
                            parents = []
                            for outparent in outfiles[parent_sect]:
                                parents.append(miscutils.parse_fullname(outparent,
                                                                        miscutils.CU_PARSE_FILENAME))
                            wdf[key][provdefs.PROV_PARENTS] = provdefs.PROV_DELIM.join(parents)


                if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                    miscutils.fwdebug_print("INFO: after wdf = %s" % prov[provdefs.PROV_WDF],
                                            WRAPPER_OUTPUT_PREFIX)
            if len(wdf) == 0:
                del prov[provdefs.PROV_WDF]

        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: End (num_errs = %d)" % num_errs, WRAPPER_OUTPUT_PREFIX)

        self.end_exec_task(num_errs)
        return prov


    ######################################################################
    def write_outputwcl(self, outfilename=None):
        """ Write output wcl to file """

        if outfilename is None:
            outfilename = self.inputwcl['wrapper']['outputwcl']

        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("outfilename = %s" % outfilename, WRAPPER_OUTPUT_PREFIX)

        # create output wcl directory if needed
        outwcldir = miscutils.parse_fullname(outfilename, miscutils.CU_PARSE_PATH)
        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("outwcldir = %s" % outwcldir, WRAPPER_OUTPUT_PREFIX)
        miscutils.coremakedirs(outwcldir)

        with open(outfilename, 'w') as wclfh:
            self.outputwcl.write(wclfh, True)


    ######################################################################
    def start_exec_task(self, name):
        """ Save start execution info """
        self.curr_task.append(name)
        self.curr_exec['task_info'][name] = {'start_time': time.time()}

    ######################################################################
    def end_exec_task(self, status):
        """ Save end execution info """
        name = self.curr_task.pop()

        task_info = self.curr_exec['task_info'][name]
        task_info['status'] = status
        task_info['end_time'] = time.time()

        # just for human reading convenience
        task_info['walltime'] = task_info['end_time'] - task_info['start_time']


    ######################################################################
    def end_all_tasks(self, status):
        """ End all exec tasks in case of exiting nested tasks """
        end_time = time.time()
        for name in reversed(self.curr_task):
            task_info = self.curr_exec['task_info'][name]
            task_info['status'] = status
            task_info['end_time'] = end_time

            # just for human reading convenience
            task_info['walltime'] = task_info['end_time'] - task_info['start_time']

        self.curr_task = []


    ######################################################################
    def save_outputs_by_section(self, ekey, outexist):
        """ save fullnames from outexist to outputs by section """
        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: before adding  outputs_by_sect=%s" % \
                                    (self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT]),
                                    WRAPPER_OUTPUT_PREFIX)
        for exlabel, exlist in outexist.items():
            if len(exlist) != 0:
                if exlabel not in self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT]:
                    self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT][exlabel] = {}
                if ekey not in self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT][exlabel]:
                    self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT][exlabel][ekey] = []

                if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
                    miscutils.fwdebug_print("INFO: adding to sect=%s: %s" % (exlabel, exlist),
                                            WRAPPER_OUTPUT_PREFIX)
                self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT][exlabel][ekey].extend(exlist)
            else:
                miscutils.fwdebug_print("WARN: 0 output files in exlist for %s" % (exlabel),
                                        WRAPPER_OUTPUT_PREFIX)


        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: after adding  outputs_by_sect=%s" % \
                                    (self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT]),
                                    WRAPPER_OUTPUT_PREFIX)


    ######################################################################
    def cleanup(self):
        """ Remove intermediate files from wrapper execution """
        self.outputwcl['wrapper']['cleanup_start'] = time.time()
        self.outputwcl['wrapper']['cleanup_end'] = time.time()

    ######################################################################
    def run_wrapper(self):
        """ Workflow for this wrapper """
        if miscutils.fwdebug_check(3, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: Begin", WRAPPER_OUTPUT_PREFIX)
        self.outputwcl['wrapper']['start_time'] = time.time()
        try:
            execs = intgmisc.get_exec_sections(self.inputwcl, intgdefs.IW_EXEC_PREFIX)
            if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("INFO:  exec sections = %s" % execs, WRAPPER_OUTPUT_PREFIX)

            for ekey, iw_exec in sorted(execs.items()):
                ow_exec = {'task_info': {}}
                self.outputwcl[ekey] = ow_exec
                self.curr_exec = ow_exec

                self.transform_inputs(iw_exec)
                inputs = self.check_inputs(ekey)
                self.check_command_line(ekey, iw_exec)
                self.save_exec_version(iw_exec)
                self.create_command_line(ekey, iw_exec)
                self.create_output_dirs(iw_exec)
                self.run_exec()
                self.transform_outputs(iw_exec)
                outexist = self.check_outputs(ekey, ow_exec['status'])
                self.save_outputs_by_section(ekey, outexist)
                self.save_provenance(ekey, iw_exec, inputs, outexist, ow_exec['status'])

                ow_exec['status'] = 0

            self.cleanup()
            self.outputwcl['wrapper']['status'] = self.determine_status()
        except SystemExit as e:
            miscutils.fwdebug_print("INFO: wrapper called sys.exit (%s).  Halting." % str(e), WRAPPER_OUTPUT_PREFIX)
            self.outputwcl['wrapper']['status'] = int(str(e))
            self.end_all_tasks(1)
        except Exception:
            (exc_type, exc_value, exc_trback) = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_trback,
                                      file=sys.stdout)
            self.outputwcl['wrapper']['status'] = 1
            self.end_all_tasks(1)


        if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
            miscutils.fwdebug_print("INFO: outputwcl[intgdefs.OW_OUTPUTS_BY_SECT]=%s" % \
                                    (self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT]),
                                    WRAPPER_OUTPUT_PREFIX)
        for fsname, fssect in self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT].items():
            if miscutils.fwdebug_check(6, 'BASICWRAP_DEBUG'):
                miscutils.fwdebug_print("INFO: making string for sect %s: %s" % (fsname, fssect),
                                        WRAPPER_OUTPUT_PREFIX)
            for exname, exlist in fssect.items():
                self.outputwcl[intgdefs.OW_OUTPUTS_BY_SECT][fsname][exname] = provdefs.PROV_DELIM.join(exlist)
        self.outputwcl['wrapper']['end_time'] = time.time()

        miscutils.fwdebug_print("INFO: end - exit status = %s" % self.get_status() , WRAPPER_OUTPUT_PREFIX)
