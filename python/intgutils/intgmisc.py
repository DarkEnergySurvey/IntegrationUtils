
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
        raise ValueError(f'Invalid cmd hyphen type ({hyphen_type})')

    return hyphen

#######################################################################
def get_exec_sections(wcl, prefix):
    """ Returns exec sections appearing in given wcl """
    execs = {}
    for key, val in wcl.items():
        if miscutils.fwdebug_check(3, "DEBUG"):
            miscutils.fwdebug_print(f"\tsearching for exec prefix in {key}")

        if re.search(r"^%s\d+$" % prefix, key):
            if miscutils.fwdebug_check(4, "DEBUG"):
                miscutils.fwdebug_print(f"\tFound exec prefex {key}")
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

    subp = subprocess4.Popen(shlex.split(cmd), shell=False, text=True)
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
                miscutils.fwdie(f"Error: invalid FMT column: {col}", 1)
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
        miscutils.fwdebug_print(f'colstr={colstr}')

    columns = convert_col_string_to_list(colstr, False)

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print(f'columns={columns}')

    fullnames = {}
    pos2fsect = {}
    for i, col in enumerate(columns):
        lcol = col.lower()
        if lcol.endswith('.fullname'):
            filesect = lcol[:-9]
            pos2fsect[i] = filesect
            fullnames[filesect] = []
        # else a data column instead of a filename

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print(f'pos2fsect={pos2fsect}')

    if linefmt in ['config', 'wcl']:
        miscutils.fwdie(f'Error:  wcl list format not currently supported ({listfile})', 1)
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
                    miscutils.fwdie(f'Error:  unknown linefmt ({linefmt})', 1)

                # save each fullname in line
                for pos in pos2fsect:
                    # use common routine to parse actual fullname (e.g., remove [0])
                    parsemask = miscutils.CU_PARSE_PATH | miscutils.CU_PARSE_FILENAME | \
                                miscutils.CU_PARSE_COMPRESSION
                    (path, filename, compression) = miscutils.parse_fullname(lineinfo[pos],
                                                                             parsemask)
                    fname = f"{path}/{filename}"
                    if compression is not None:
                        fname += compression
                    fullnames[pos2fsect[pos]].append(fname)

    if miscutils.fwdebug_check(6, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print(f'fullnames = {fullnames}')
    return fullnames


######################################################################
def get_list_fullnames(sect, modwcl):
    """ get list of full names
    """
    (_, listsect, filesect) = sect.split('.')
    ldict = modwcl[intgdefs.IW_LIST_SECT][listsect]

    # check list itself exists
    listname = ldict['fullname']
    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print(f"\tINFO: Checking existence of '{listname}'")

    if not os.path.exists(listname):
        miscutils.fwdebug_print(f"\tError: input list '{listname}' does not exist.")
        raise IOError(f"List not found: {listname} does not exist")

    # get list format: space separated, csv, wcl, etc
    listfmt = intgdefs.DEFAULT_LIST_FORMAT
    if intgdefs.LIST_FORMAT in ldict:
        listfmt = ldict[intgdefs.LIST_FORMAT]

    setfnames = set()

    # read fullnames from list file
    fullnames = read_fullnames_from_listfile(listname, listfmt, ldict['columns'])
    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print(f"\tINFO: fullnames={fullnames}")

    if filesect not in fullnames:
        columns = convert_col_string_to_list(ldict['columns'], False)

        if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
            miscutils.fwdebug_print('columns={columns}')

        hasfullname = False
        for col in columns:
            lcol = col.lower()
            if lcol.endswith('.fullname') and lcol.startswith(filesect):
                hasfullname = True
        if hasfullname:
            miscutils.fwdebug_print(f"ERROR: Could not find sect {filesect} in list")
            miscutils.fwdebug_print(f"\tcolumns = {columns}")
            miscutils.fwdebug_print(f"\tlist keys = {list(fullnames.keys())}")
        elif miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
            miscutils.fwdebug_print(f"WARN: Could not find sect {filesect} in fullname list.   Not a problem if list (sect) has only data.")
    else:
        setfnames = set(fullnames[filesect])
    return listname, setfnames


######################################################################
def get_file_fullnames(sect, filewcl, fullwcl):
    """ get list of full names """
    sectkeys = sect.split('.')
    sectname = sectkeys[1]

    if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
        miscutils.fwdebug_print(f"INFO: Beg sectname={sectname}")

    fnames = []
    if sectname in filewcl:
        filesect = filewcl[sectname]
        if 'fullname' in filesect:
            fnames = replfuncs.replace_vars(filesect['fullname'], fullwcl)[0]
            fnames = miscutils.fwsplit(fnames, ',')
            if miscutils.fwdebug_check(3, 'INTGMISC_DEBUG'):
                miscutils.fwdebug_print(f"INFO: fullname = {fnames}")

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
    for _exsect in sorted(exec_sectnames):
        exwcl = modwcl[_exsect]
        if intgdefs.IW_OUTPUTS in exwcl:
            for sect in miscutils.fwsplit(exwcl[intgdefs.IW_OUTPUTS], ','):
                sectkeys = sect.split('.')
                outset = None
                if sectkeys[0] == intgdefs.IW_FILE_SECT:
                    outset = get_file_fullnames(sect, modwcl[intgdefs.IW_FILE_SECT], fullwcl)
                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    _, outset = get_list_fullnames(sect, modwcl)
                else:
                    print("exwcl[intgdefs.IW_OUTPUTS]=", exwcl[intgdefs.IW_OUTPUTS])
                    print("sect = ", sect)
                    print("sectkeys = ", sectkeys)
                    raise KeyError(f"Unknown data section {sectkeys[0]}")
                outputs[sect] = outset
                allouts.union(outset)

    inputs = {}
    for _exsect in exec_sectnames:
        exwcl = modwcl[_exsect]
        if intgdefs.IW_INPUTS in exwcl:
            for sect in miscutils.fwsplit(exwcl[intgdefs.IW_INPUTS], ','):
                sectkeys = sect.split('.')
                inset = None
                if sectkeys[0] == intgdefs.IW_FILE_SECT:
                    inset = get_file_fullnames(sect, modwcl[intgdefs.IW_FILE_SECT], fullwcl)
                elif sectkeys[0] == intgdefs.IW_LIST_SECT:
                    _, inset = get_list_fullnames(sect, modwcl)
                    #inset.add(listname)
                else:
                    print("exwcl[intgdefs.IW_INPUTS]=", exwcl[intgdefs.IW_INPUTS])
                    print("sect = ", sect)
                    print("sectkeys = ", sectkeys)
                    raise KeyError(f"Unknown data section {sectkeys[0]}")

                # exclude intermediate files from inputs
                if inset is not None:
                    inset = inset - allouts
                    inputs[sect] = inset

    return inputs, outputs


######################################################################
def check_input_files(sect, filewcl):
    """ Check that the files for a single input file section exist """

    sectkeys = sect.split('.')
    fnames = miscutils.fwsplit(filewcl[sectkeys[1]]['fullname'], ',')
    (exists1, missing1) = check_files(fnames)
    return (exists1, missing1)
