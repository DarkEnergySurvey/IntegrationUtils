#!/usr/bin/env python

# $Id: intgmisc.py 44380 2016-10-11 19:12:41Z mgower $
# $Rev:: 44380                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-10-11 14:12:41 #$:  # Date of last commit.

"""
Contains misc integration utilities
"""

import shlex
import os
import re
from despymisc import subprocess4
from despymisc import miscutils
from intgutils import intgdefs
import intgutils.replace_funcs as replfuncs


######################################################################
def check_files(fullnames):
    """ Check whether given files do exist on disk """

    exists = []
    missing = []
    for fname in fullnames:
        if os.path.exists(fname):
            exists.append(fname)
        else:
            missing.append(fname)
    return (exists, missing)


#######################################################################
def get_cmd_hyphen(hyphen_type, cmd_option):
    """ Determine correct hyphenation for command line argument """

    hyphen = '-'

    if hyphen_type == 'alldouble':
        hyphen = '--'
    elif hyphen_type == 'allsingle':
        hyphen = '-'
    elif hyphen_type == 'mixed_gnu':
        if len(cmd_option) == 1:
            hyphen = '-'
        else:
            hyphen = '--'
    else:
        raise ValueError('Invalid cmd hyphen type (%s)' % hyphen_type)

    return hyphen

#######################################################################
def get_exec_sections(wcl, prefix):
    """ Returns exec sections appearing in given wcl """
    execs = {}
    for key, val in wcl.items():
        if miscutils.fwdebug_check(3, "DEBUG"):
            miscutils.fwdebug_print("\tsearching for exec prefix in %s" % key)

        if re.search(r"^%s\d+$" % prefix, key):
            if miscutils.fwdebug_check(4, "DEBUG"):
                miscutils.fwdebug_print("\tFound exec prefex %s" % key)
            execs[key] = val
    return execs


#######################################################################
def run_exec(cmd):
    """ Run an executable with given command returning process information """

    procfields = ['ru_idrss', 'ru_inblock', 'ru_isrss', 'ru_ixrss',
                  'ru_majflt', 'ru_maxrss', 'ru_minflt', 'ru_msgrcv',
                  'ru_msgsnd', 'ru_nivcsw', 'ru_nsignals', 'ru_nswap',
                  'ru_nvcsw', 'ru_oublock', 'ru_stime', 'ru_utime']
    retcode = None
    procinfo = None

    subp = subprocess4.Popen(shlex.split(cmd), shell=False)
    retcode = subp.wait4()
    procinfo = dict((field, getattr(subp.rusage, field)) for field in procfields)

    return (retcode, procinfo)


#######################################################################
def remove_column_format(columns):
    """ Return columns minus any formatting specification """

    columns2 = []
    for col in columns:
        if col.startswith('$FMT{'):
            rmatch = re.match(r'\$FMT\{\s*([^,]+)\s*,\s*(\S+)\s*\}', col)
            if rmatch:
                columns2.append(rmatch.group(2).strip())
            else:
                miscutils.fwdie("Error: invalid FMT column: %s" % (col), 1)
        else:
            columns2.append(col)
    return columns2


#######################################################################
def convert_col_string_to_list(colstr, with_format=True):
    """ convert a string of columns to list of columns """
    columns = re.findall(r'\$\S+\{.*\}|[^,\s]+', colstr)

    if not with_format:
        columns = remove_column_format(columns)
    return columns


#######################################################################
def read_fullnames_from_listfile(listfile, linefmt, colstr):
    """ Read a list file returning fullnames from the list """

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('colstr=%s' % colstr)

    columns = convert_col_string_to_list(colstr, False)

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('columns=%s' % columns)

    fullnames = {}
    pos2fsect = {}
    for pos in range(0, len(columns)):
        lcol = columns[pos].lower()
        if lcol.endswith('.fullname'):
            filesect = lcol[:-9]
            pos2fsect[pos] = filesect
            fullnames[filesect] = []
        # else a data column instead of a filename

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('pos2fsect=%s' % pos2fsect)

    if linefmt == 'config' or linefmt == 'wcl':
        miscutils.fwdie('Error:  wcl list format not currently supported (%s)' % listfile, 1)
    else:
        with open(listfile, 'r') as listfh:
            for line in listfh:
                line = line.strip()

                # convert line into python list
                lineinfo = []
                if linefmt == 'textcsv':
                    lineinfo = miscutils.fwsplit(line, ',')
                elif linefmt == 'texttab':
                    lineinfo = miscutils.fwsplit(line, '\t')
                elif linefmt == 'textsp':
                    lineinfo = miscutils.fwsplit(line, ' ')
                else:
                    miscutils.fwdie('Error:  unknown linefmt (%s)' % linefmt, 1)

                # save each fullname in line
                for pos in pos2fsect:
                    # use common routine to parse actual fullname (e.g., remove [0])
                    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | \
                                miscutils.CU_PARSE_COMPRESSION
                    (path, filename, compression) = miscutils.parse_fullname(lineinfo[pos],
                                                                             parsemask)
                    fname = "%s/%s" % (path, filename)
                    if compression is not None:
                        fname += compression
                    fullnames[pos2fsect[pos]].append(fname)

    if miscutils.fwdebug_check(6, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print('fullnames = %s' % fullnames)
    return fullnames


######################################################################
def get_list_fullnames(sect, modwcl):

    (_, listsect, filesect) = sect.split('.')
    ldict = modwcl[intgdefs.IW_LIST_SECT][listsect]

    # check list itself exists
    listname = ldict['fullname']
    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print("\tINFO: Checking existence of '%s'" % listname)

    if not os.path.exists(listname):
        miscutils.fwdebug_print("\tError: input list '%s' does not exist." % listname)
        raise IOError("List not found: %s does not exist" % listname)

    # get list format: space separated, csv, wcl, etc
    listfmt = intgdefs.DEFAULT_LIST_FORMAT
    if intgdefs.LIST_FORMAT in ldict:
        listfmt = ldict[intgdefs.LIST_FORMAT]

    setfnames = set()

    # read fullnames from list file
    fullnames = read_fullnames_from_listfile(listname, listfmt, ldict['columns'])
    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print("\tINFO: fullnames=%s" % fullnames)
   
    if filesect not in fullnames:
        columns = convert_col_string_to_list(ldict['columns'], False)

        if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
            miscutils.fwdebug_print('columns=%s' % columns)

        hasfullname = False
        for pos in range(0, len(columns)):
            lcol = columns[pos].lower()
            if lcol.endswith('.fullname') and lcol.startswith(filesect):
                hasfullname = True
        if hasfullname:
            miscutils.fwdebug_print("ERROR: Could not find sect %s in list" % (filesect))
            miscutils.fwdebug_print("\tcolumns = %s" % (columns))
            miscutils.fwdebug_print("\tlist keys = %s" % (fullnames.keys()))
        elif miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
            miscutils.fwdebug_print("WARN: Could not find sect %s in fullname list.   Not a problem if list (sect) has only data." % (filesect))
    else:
        setfnames = set(fullnames[filesect])
    return listname, setfnames


######################################################################
def get_file_fullnames(sect, filewcl, fullwcl):

    sectkeys = sect.split('.')
    sectname = sectkeys[1]

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print("INFO: Beg sectname=%s" % sectname)

    fnames = []
    if sectname in filewcl:
        filesect = filewcl[sectname]
        if 'fullname' in filesect:
            fnames = replfuncs.replace_vars(filesect['fullname'], fullwcl)[0]
            fnames = miscutils.fwsplit(fnames, ',')
            if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
                miscutils.fwdebug_print("INFO: fullname = %s" % fnames)

    return set(fnames)



######################################################################
def get_fullnames(modwcl, fullwcl, exsect=None):
    """ Return dictionaries of input and output fullnames by section """

    exec_sectnames = []
    if exsect is None: 
        exec_sectnames = get_exec_sections(modwcl, intgdefs.IW_EXEC_PREFIX)
    else:
        exec_sectnames = [exsect]

    # intermediate files (output of 1 exec, but input for another exec 
    # within same wrapper) are listed only with output files 
    
    # get output file names first so can exclude intermediate files from inputs
    outputs = {}
    allouts = set()
    for exsect in sorted(exec_sectnames):
        exwcl = modwcl[exsect]
        if intgdefs.IW_OUTPUTS in exwcl:
            for sect in miscutils.fwsplit(exwcl[intgdefs.IW_OUTPUTS], ','):
                sectkeys = sect.split('.')
                outset = None
                if sectkeys[0] == intgdefs.IW_FILE_SECT:
                    outset = get_file_fullnames(sect, modwcl[intgdefs.IW_FILE_SECT], fullwcl)
                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    listname, outset = get_list_fullnames(sect, modwcl)
                else:
                    print "exwcl[intgdefs.IW_OUTPUTS]=", exwcl[intgdefs.IW_OUTPUTS]
                    print "sect = ", sect
                    print "sectkeys = ", sectkeys
                    raise KeyError("Unknown data section %s" % sectkeys[0])
                outputs[sect] = outset
                allouts.union(outset)

    inputs = {}
    for exsect in sorted(exec_sectnames):
        exwcl = modwcl[exsect]
        if intgdefs.IW_INPUTS in exwcl:
            for sect in miscutils.fwsplit(exwcl[intgdefs.IW_INPUTS], ','):
                sectkeys = sect.split('.')
                inset = None
                if sectkeys[0] == intgdefs.IW_FILE_SECT:
                    inset = get_file_fullnames(sect, modwcl[intgdefs.IW_FILE_SECT], fullwcl)
                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    listname, inset = get_list_fullnames(sect, modwcl)
                    #inset.add(listname)
                else:
                    print "exwcl[intgdefs.IW_INPUTS]=", exwcl[intgdefs.IW_INPUTS]
                    print "sect = ", sect
                    print "sectkeys = ", sectkeys
                    raise KeyError("Unknown data section %s" % sectkeys[0])

                # exclude intermediate files from inputs
                if inset is not None:
                    inset = inset - allouts
                    inputs[sect] = inset

    return inputs, outputs


######################################################################
#def check_list(sect, listwcl, filewcl):
#    """ Check that list and files inside list exist """
#
#    existfiles = {}
#    missingfiles = []
#    (_, listsect, filesect) = sect.split('.')
#
#    ldict = listwcl[listsect]
#
#    # check list itself exists
#    listname = ldict['fullname']
#    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
#        miscutils.fwdebug_print("\tINFO: Checking existence of '%s'" % listname)
#
#    if not os.path.exists(listname):
#        miscutils.fwdebug_print("\tError: input list '%s' does not exist." % listname)
#        raise IOError("List not found: %s does not exist" % listname)
#
#    list_filename = miscutils.parse_fullname(listname, miscutils.CU_PARSE_FILENAME)
#
#    # get list format: space separated, csv, wcl, etc
#    listfmt = intgdefs.DEFAULT_LIST_FORMAT
#    if intgdefs.LIST_FORMAT in ldict:
#        listfmt = ldict[intgdefs.LIST_FORMAT]
#
#    # read fullnames from list file
#    fullnames = read_fullnames_from_listfile(listname, listfmt, ldict['columns'])
#    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
#        miscutils.fwdebug_print("\tINFO: fullnames=%s" % fullnames)
#
#    if filesect in fullnames:
#        existfiles, missingfiles = check_files(fullnames[filesect], filewcl)
#
#    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
#        miscutils.fwdebug_print("\tINFO: exists=%s" % existfiles)
#        miscutils.fwdebug_print("\tINFO: missing=%s" % missingfiles)
#
#    return list_filename, existfiles, missingfiles
#

######################################################################
#def check_input_list(sect, listwcl, filewcl):
#    """ Check that the list and contained files for a single input list section exist """
#
#    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
#        miscutils.fwdebug_print("INFO: Beg sect=%s" % sect)
#
#    list_filename, existfiles, missingfiles = check_list(sect, listwcl, filewcl)
#
#    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
#        miscutils.fwdebug_print("\tINFO: exists=%s" % existfiles)
#        miscutils.fwdebug_print("\tINFO: missing=%s" % missingfiles)
#
#    return list_filename, existfiles, missingfiles
#


######################################################################
def check_input_files(sect, filewcl):
    """ Check that the files for a single input file section exist """

    sectkeys = sect.split('.')
    fnames = miscutils.fwsplit(filewcl[sectkeys[1]]['fullname'], ',')
    (exists1, missing1) = check_files(fnames)
    return (exists1, missing1)



######################################################################
#def check_exec_inputs(exwcl, listwcl, filewcl):
#    """ Check that inputs exist for module exec """
#
#    already_checked_list = {}
#    existfiles = {}
#    missingfiles = []
#
#    for sect in miscutils.fwsplit(exwcl[intgdefs.IW_INPUTS], ','):
#        sectkeys = sect.split('.')
#        if sect not in already_checked_list:
#            if sectkeys[0] == intgdefs.IW_FILE_SECT:
#                (exists, missing) = check_input_files(sect, filewcl)
#            elif sectkeys[0] == intgdefs.IW_LIST_SECT:
#                (list_filename, exists, missing) = check_input_list(sect, listwcl, filewcl)
#
#                # save that list exists
#                sectkeys = sect.split('.')
#                existfiles['%s.%s'%(intgdefs.IW_LIST_SECT, sectkeys[1])] = [list_filename]
#            else:
#                print "exwcl[intgdefs.IW_INPUTS]=", exwcl[intgdefs.IW_INPUTS]
#                print "sect = ", sect
#                print "sectkeys = ", sectkeys
#                raise KeyError("Unknown data section %s" % sectkeys[0])
#
#            already_checked_list[sect] = True
#            existfiles.update({sect: exists})
#            missingfiles.extend(missing)
#
#    return existfiles, missingfiles
