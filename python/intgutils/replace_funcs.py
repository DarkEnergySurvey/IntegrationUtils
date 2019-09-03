#!/usr/bin/env python
# $Id: replace_funcs.py 41600 2016-04-05 20:33:30Z mgower $
# $Rev:: 41600                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-04-05 15:33:30 #$:  # Date of last commit.

# pylint: disable=print-statement

""" Functions to replace variables in a string with their values from a isinstance(dict) object """

import copy
import re
import pyfits

import despymisc.miscutils as miscutils
import intgutils.intgdefs as intgdefs
import despyfitsutils.fitsutils as fitsutils

def replace_vars_single(instr, valdict, opts=None):
    """ Return single instr after replacing vars """

    assert(isinstance(instr, str))
    #assert(isinstance(valdict, dict))

    values, keeps = replace_vars(instr, valdict, opts)

    retval = None
    if isinstance(values, list):
        if len(values) == 1:
            retval = values[0]
        else:
            miscutils.fwdebug_print("Error:  Multiple results when calling replace_vars_single")
            miscutils.fwdebug_print("\tinstr = %s" % instr)
            miscutils.fwdebug_print("\tvalues = %s" % values)
            raise KeyError("Error: Single search failed (%s)" % instr)
    else:
        retval = values

    return retval


def replace_vars_type(instr, valdict, required, stype, opts=None):
    """ Search given string for variables of 1 type and replace """

    assert(isinstance(instr, str))
    #assert(isinstance(valdict, dict))

    keep = {}
    done = True
    maxtries = 100    # avoid infinite loop
    count = 0

    newstr = copy.copy(instr)

    # be careful of nested variables  ${RMS_${BAND}}
    varpat = r"(?i)\$%s\{([^$}]+)\}" % stype

    match_var = re.search(varpat, newstr)
    while match_var and count < maxtries:
        count += 1

        # the string inside the curly braces
        var = match_var.group(1)

        # may be var:#
        parts = var.split(':')

        # variable name to replace
        newvar = parts[0]

        if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
            miscutils.fwdebug_print("\t newstr: %s " % (newstr))
            miscutils.fwdebug_print("\t var: %s " % (var))
            miscutils.fwdebug_print("\t parts: %s " % (parts))
            miscutils.fwdebug_print("\t newvar: %s " % (newvar))

        # find the variable's value
        if stype == 'HEAD':
            if miscutils.fwdebug_check(0, 'REPL_DEBUG'):
                miscutils.fwdebug_print("\tfound HEAD variable to expand: %s " % (newvar))

            varlist = miscutils.fwsplit(newvar, ',')
            fname = varlist[0]
            if miscutils.fwdebug_check(0, 'REPL_DEBUG'):
                miscutils.fwdebug_print("\tHEAD variable fname: %s " % (fname))
            hdulist = pyfits.open(fname, 'readonly')
            newval = []
            for key in varlist[1:]:
                if miscutils.fwdebug_check(0, 'REPL_DEBUG'):
                    miscutils.fwdebug_print("\tHEAD variable header key: %s " % (key))
                newval.append(str(fitsutils.get_hdr_value(hdulist, key)))
            miscutils.fwdebug_print("\tnewval: %s " % (newval))
            newval = ','.join(newval)
            haskey = True
            hdulist.close()
        elif stype == 'FUNC':
            if miscutils.fwdebug_check(0, 'REPL_DEBUG'):
                miscutils.fwdebug_print("\tfound FUNC variable to expand: %s " % (newvar))

            varlist = miscutils.fwsplit(newvar, ',')
            funcinfo = varlist[0]
            if miscutils.fwdebug_check(0, 'REPL_DEBUG'):
                miscutils.fwdebug_print("\tFUNC info: %s " % (funcinfo))

            specf = miscutils.dynamically_load_class(funcinfo)
            newval = specf(varlist[1:])
            haskey = True
        elif hasattr(valdict, 'search'):
            (haskey, newval) = valdict.search(newvar, opts)
        else:
            haskey = False
            if newvar in valdict:
                haskey = True
                newval = valdict[newvar]

        if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
            miscutils.fwdebug_print("\t newvar: %s " % (newvar))
            miscutils.fwdebug_print("\t haskey: %s " % (haskey))
            miscutils.fwdebug_print("\t newval: %s " % (newval))

        if haskey:
            newval = str(newval)

            # check if a multiple value variable (e.g., band, ccdnum)
            if newval.startswith('(') or ',' in newval:
                if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                    miscutils.fwdebug_print("\tfound val to expand: %s " % (newval))
                    miscutils.fwdebug_print("\tfound val to expand: opts=%s " % (opts))

                if opts is not None and 'expand' in opts and opts['expand']:
                    newval = '$LOOP{%s}' % var   # postpone for later expanding

                if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                    miscutils.fwdebug_print("\tLOOP? newval = %s" % newval)
            elif len(parts) > 1:
                prpat = "%%0%dd" % int(parts[1])
                try:
                    keepval = replace_vars_single(newval, valdict, opts)
                    keep[newvar] = keepval
                    newval = prpat % int(keepval)
                except (TypeError, ValueError) as err:
                    miscutils.fwdebug_print("\tError = %s" % str(err))
                    miscutils.fwdebug_print("\tprpat = %s" % prpat)
                    miscutils.fwdebug_print("\tnewval = %s" % newval)
                    miscutils.fwdebug_print("\topts = %s" % opts)
                    raise err
            else:
                keep[newvar] = newval

            newstr = re.sub(r"(?i)\$%s{%s}" % (stype, var), newval, newstr)
            done = False
        elif required:
            raise KeyError("Error: Could not find value for %s" % newvar)
        else:
            # missing optional value so replace with empty string
            newstr = re.sub(r"(?i)\$%s{%s}" % (stype, var), "", newstr)

        match_var = re.search(varpat, newstr)

    return (done, newstr, keep)


def replace_vars_loop(valpair, valdict, opts=None):
    """ Expand variables that have multiple values (e.g., band, ccdnum) """

    #assert(isinstance(valdict, dict))

    looptodo = [valpair]
    valuedone = []
    keepdone = []
    maxtries = 100    # avoid infinite loop
    count = 0
    while len(looptodo) > 0 and count < maxtries:
        count += 1
        valpair = looptodo.pop()

        if miscutils.fwdebug_check(3, 'REPL_DEBUG'):
            miscutils.fwdebug_print("looptodo: valpair[0] = %s" % valpair[0])

        match_loop = re.search(r"(?i)\$LOOP\{([^}]+)\}", valpair[0])

        var = match_loop.group(1)
        parts = var.split(':')
        newvar = parts[0]

        if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
            miscutils.fwdebug_print("\tloop search: newvar= %s" % newvar)
            miscutils.fwdebug_print("\tloop search: opts= %s" % opts)

        (haskey, newval,  ) = valdict.search(newvar, opts)

        if haskey:
            if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                miscutils.fwdebug_print("\tloop search results: newva1= %s" % newval)

            newvalarr = miscutils.fwsplit(newval)
            for nval in newvalarr:
                if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                    miscutils.fwdebug_print("\tloop nv: nval=%s" % nval)

                kval = nval    # save unpadded value for keep
                if len(parts) > 1:
                    try:
                        prpat = "%%0%dd" % int(parts[1])
                        nval = prpat % int(nval)
                    except (TypeError, ValueError) as err:
                        miscutils.fwdebug_print("\tError = %s" % str(err))
                        miscutils.fwdebug_print("\tprpat = %s" % prpat)
                        miscutils.fwdebug_print("\tnval = %s" % nval)
                        miscutils.fwdebug_print("\topts = %s" % opts)
                        raise err

                if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                    miscutils.fwdebug_print("\tloop nv2: nval=%s" % nval)
                    miscutils.fwdebug_print("\tbefore loop sub: valpair[0]=%s" % valpair[0])

                valsub = re.sub(r"(?i)\$LOOP\{%s\}" % var, nval, valpair[0])
                keep = copy.deepcopy(valpair[1])
                keep[newvar] = kval
                if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                    miscutils.fwdebug_print("\tafter loop sub: valsub=%s" % valsub)
                if '$LOOP{' in valsub:
                    if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                        miscutils.fwdebug_print("\t\tputting back in todo list")
                    looptodo.append((valsub, keep))
                else:
                    valuedone.append(valsub)
                    keepdone.append(keep)
                    if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
                        miscutils.fwdebug_print("\t\tputting back in done list")
        if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
            miscutils.fwdebug_print("\tNumber in todo list = %s" % len(looptodo))
            miscutils.fwdebug_print("\tNumber in done list = %s" % len(valuedone))
    if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
        miscutils.fwdebug_print("\tEND OF WHILE LOOP = %s" % len(valuedone))

    return valuedone, keepdone


def replace_vars(instr, valdict, opts=None):
    """ Replace variables in given instr """

    assert(isinstance(instr, str))
    #assert(isinstance(valdict, dict))

    newstr = copy.copy(instr)

    if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("\tinitial instr = '%s'" % instr)
        #miscutils.fwdebug_print("\tvaldict = '%s'" % valdict)
        miscutils.fwdebug_print("\tinitial opts = '%s'" % opts)

    keep = {}

    maxtries = 100    # avoid infinite loop
    count = 0
    done = False
    while not done and count < maxtries:
        count += 1
        done = True


        # header vars ($HEAD{)
        (done2, newstr, keep2) = replace_vars_type(newstr, valdict, True, 'HEAD', opts)
        done = done and done2
        keep.update(keep2)

        # optional vars ($opt{)
        (done2, newstr, keep2) = replace_vars_type(newstr, valdict, False, 'opt', opts)
        done = done and done2
        keep.update(keep2)

        # required vars (${)
        (done2, newstr, keep2) = replace_vars_type(newstr, valdict, True, '', opts)
        done = done and done2
        keep.update(keep2)

    #print "keep = ", keep

    if count >= maxtries:
        raise Exception("Error: replace_vars function aborting from infinite loop '%s'" % instr)

    ##### FUNC
    maxtries = 100    # avoid infinite loop
    count = 0
    done = False
    while not done and count < maxtries:
        count += 1
        done = True

        # func vars ($FUNC{)
        (done2, newstr, keep2) = replace_vars_type(newstr, valdict, True, 'FUNC', opts)
        done = done and done2
        keep.update(keep2)

    #print "keep = ", keep

    if count >= maxtries:
        raise Exception("Error: replace_vars function aborting from infinite loop '%s'" % instr)


    #####
    valpair = (newstr, keep)
    valuedone = []
    keepdone = []
    if '$LOOP' in newstr:
        if opts is not None:
            opts['required'] = True
        else:
            opts = {'required': True, intgdefs.REPLACE_VARS: False}
        valuedone, keepdone = replace_vars_loop(valpair, valdict, opts)


    if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
        miscutils.fwdebug_print("\tvaluedone = %s" % valuedone)
        miscutils.fwdebug_print("\tkeepdone = %s" % keepdone)
        miscutils.fwdebug_print("\tvaluepair = %s" % str(valpair))
        miscutils.fwdebug_print("\tinstr = %s" % instr)

    val2return = None
    if len(valuedone) >= 1:
        val2return = valuedone, keepdone
    else:
        val2return = valpair

    if miscutils.fwdebug_check(6, 'REPL_DEBUG'):
        miscutils.fwdebug_print("\tval2return = %s" % str(val2return))
    if miscutils.fwdebug_check(5, 'REPL_DEBUG'):
        miscutils.fwdebug_print("END")
    return val2return

