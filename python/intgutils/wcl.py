# $Id: wcl.py 41753 2016-04-26 13:12:09Z mgower $
# $Rev:: 41753                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-04-26 08:12:09 #$:  # Date of last commit.

# pylint: disable=print-statement

"""
Contains utilities for use with the Workflow Control Language
"""

# Notes:
#     * position of <<include matters, all includes not processed first
#     * includes do not expand wildcards

import sys
import re
import os
import collections
from importlib import import_module
import copy


import despymisc.miscutils as miscutils
import intgutils.intgdefs as intgdefs
import intgutils.replace_funcs as replfuncs

class WCL(collections.OrderedDict):
    """ Base WCL class """

    def __init__(self, *args, **kwds):
        """ Initialize with given wcl """

        collections.OrderedDict.__init__(self, *args, **kwds)
        self.search_order = collections.OrderedDict()

    ###########################################################################
    def set_search_order(self, search_order):
        """ Set the search order """

        self.search_order = search_order


    ###########################################################################
    def __contains__(self, key, opts=None):
        """ D.__contains__(k) -> True if D has a key k, else False """
        (found, _) = self.search(key, opts)
        return found

    ###########################################################################
    def get(self, key, opts=None, default=None):
        """ Gets value of key in wcl, follows search order and section notation """

        (found, value) = self.search(key, opts)
        if not found:
            value = default

        return value

    ###########################################################################
    #def __setitem__(self, key, val):
    #    """ x.__setitem__(i, y) <==> x[i]=y """
    #    OrderedDict.__setitem__(self, key, val)


    ###########################################################################
    def set(self, key, val):
        """ Sets value of key in wcl, follows section notation """

        if miscutils.fwdebug_check(9, "WCL_DEBUG"):
            miscutils.fwdebug_print(f"BEG key={key}, val={val}")

        subkeys = key.split('.')
        valkey = subkeys.pop()
        wcldict = self
        for k in subkeys:
            wcldict = collections.OrderedDict.__getitem__(wcldict, k)

        collections.OrderedDict.__setitem__(wcldict, valkey, val)

        if miscutils.fwdebug_check(9, "WCL_DEBUG"):
            miscutils.fwdebug_print("END")



    ###########################################################################
    def search(self, key, opt=None):
        """ Searches for key using given opt following hierarchy rules """

        if miscutils.fwdebug_check(8, 'WCL_DEBUG'):
            miscutils.fwdebug_print("\tBEG")
            miscutils.fwdebug_print(f"\tinitial key = '{key}'")
            miscutils.fwdebug_print(f"\tinitial opts = '{opt}'")

        curvals = None
        found = False
        value = ''
        if hasattr(key, 'lower'):
            key = key.lower()
        else:
            print(f"key = {key}")

        # if key contains period, use it exactly instead of scoping rules
        if isinstance(key, str) and '.' in key:
            if miscutils.fwdebug_check(8, 'WCL_DEBUG'):
                miscutils.fwdebug_print(f"\t. in key '{key}'")

            value = self
            found = True
            for k in key.split('.'):
                if miscutils.fwdebug_check(8, 'WCL_DEBUG'):
                    miscutils.fwdebug_print(f"\t\t partial key '{k}'")
                if k in value:
                    value = collections.OrderedDict.__getitem__(value, k)
                    if miscutils.fwdebug_check(8, 'WCL_DEBUG'):
                        miscutils.fwdebug_print(f"\t\t next val '{value}'")
                    found = True
                else:
                    value = ''
                    found = False
                    break

        else:
            # start with stored current values
            if collections.OrderedDict.__contains__(self, 'current'):
                curvals = copy.deepcopy(collections.OrderedDict.__getitem__(self, 'current'))
            else:
                curvals = collections.OrderedDict()

            # override with current values passed into function if given
            if opt is not None and 'currentvals' in opt:
                for ckey, cval in opt['currentvals'].items():
                    if miscutils.fwdebug_check(8, 'WCL_DEBUG'):
                        miscutils.fwdebug_print(f"using specified curval {ckey} = {cval}")
                    curvals[ckey] = cval

            if miscutils.fwdebug_check(6, 'WCL_DEBUG'):
                miscutils.fwdebug_print(f"curvals = {curvals}")
            if key in curvals:
                #print "found %s in curvals" % (key)
                found = True
                value = curvals[key]
            elif opt and 'searchobj' in opt and key in opt['searchobj']:
                #print '%s in searchobj' % key
                found = True
                value = opt['searchobj'][key]
            else:
                #print dir(self)
                if hasattr(self, 'search_order'):
                    for sect in self.search_order:
                        #print "Searching section %s for key %s" % (sect, key)
                        if "curr_" + sect in curvals:
                            currkey = curvals['curr_'+sect]
                            #print "\tcurrkey for section %s = %s" % (sect, currkey)
                            if collections.OrderedDict.__contains__(self, sect):
                                sectdict = collections.OrderedDict.__getitem__(self, sect)
                                if currkey in sectdict:
                                    if key in sectdict[currkey]:
                                        found = True
                                        value = sectdict[currkey][key]
                                        break

            # lastly check global values
            if not found:
                #print "\t%s not found, checking global values" % (key)
                if collections.OrderedDict.__contains__(self, key):
                    found = True
                    value = collections.OrderedDict.__getitem__(self, key)


        if not found and opt and 'required' in opt and opt['required']:
            print(f"\n\nError: search for {key} failed")
            print("\tcurrent = ", collections.OrderedDict.__getitem__(self, 'current'))
            print("\topt = ", opt)
            print("\tcurvals = ", curvals)
            print("\n\n")
            raise KeyError(f"Error: Search failed ({key})")

        if miscutils.fwdebug_check(8, 'WCL_DEBUG'):
            miscutils.fwdebug_print(f"\tEND: found={found}, value={value}")

        return found, value


    #######################################################################
    @classmethod
    def search_wcl_for_variables(cls, wcl):
        """ Search the wcl for variables """

        if miscutils.fwdebug_check(9, "WCL_DEBUG"):
            miscutils.fwdebug_print("BEG")
        usedvars = {}
        for key, val in wcl.items():
            if isinstance(val, (dict, collections.OrderedDict)):
                uvars = cls.search_wcl_for_variables(val)
                if uvars:
                    usedvars.update(uvars)
            elif isinstance(val, str):
                viter = [m.group(1) for m in re.finditer(r'(?i)\$\{([^}]+)\}', val)]
                for vstr in viter:
                    if ':' in vstr:
                        vstr = vstr.split(':')[0]
                    usedvars[vstr] = True
            else:
                if miscutils.fwdebug_check(9, "WCL_DEBUG"):
                    miscutils.fwdebug_print("Note: wcl is not string.")
                    miscutils.fwdebug_print(f"\tkey = {key}, type(val) = {type(val)}, val = '{val}'")

        if miscutils.fwdebug_check(9, "WCL_DEBUG"):
            miscutils.fwdebug_print("END")
        return usedvars




    #######################################################################
    def write(self, out_file=None, sortit=False, indent=4):
        """Outputs a given dictionary in WCL format where items within
           the same sub-dictionary are output in alphabetical order"""

        if out_file is None:
            out_file = sys.stdout

        self._recurs_write_wcl(self, out_file, sortit, indent, 0)


    def _recurs_write_wcl(self, wcl_dict, out_file, sortit, inc_indent, curr_indent):
        """Internal recursive function to do actual WCL writing"""
        if wcl_dict:
            if sortit:
                dictitems = sorted(wcl_dict.items())
            else:
                dictitems = wcl_dict.items()

            for key, value in dictitems:
                if isinstance(value, dict):
                    print(' ' * curr_indent + "<" + str(key) + ">", file=out_file)
                    save_sortit = sortit
                    if key == 'cmdline':
                        sortit = False    # don't sort cmdline section
                    self._recurs_write_wcl(value, out_file, sortit, inc_indent,
                                           curr_indent + inc_indent)
                    sortit = save_sortit
                    print(' ' * curr_indent + "</" + str(key) + ">", file=out_file)
                elif value is not None:
                    print(' ' * curr_indent + f"{str(key)} = {str(value)}", file=out_file)


    def read(self, in_file=None, cmdline=False, filename='stdin'):
        """Reads WCL text from an open file object and returns a dictionary"""

        curr = self
        stack = []  # to keep track of current sub-dictionary
        stackkeys = ['__topwcl__']  # to keep track of current section key
        stack.append(curr) #

        line = in_file.readline()
        linecnt = 1
        while line:
            line = line.strip()
            while line.endswith('\\'):
                linecnt += 1
                line = line[:-1] + in_file.readline().strip()

            # delete comments
            line = line.split('#')[0]

            # skip comment line or empty line
            if re.search(r"\S", line):
                # handle includes
                patmatch = re.search(r"<<include (\S+)>>", line)
                if patmatch is not None:
                    # replace wcl vars in filename
                    filename2 = replfuncs.replace_vars_single(patmatch.group(1), self, None)

                    # expand ~ and env vars in filename
                    filename2 = os.path.expandvars(os.path.expanduser(filename2))

                    wclobj2 = WCL()
                    with open(filename2, "r") as wclfh:
                        wclobj2.read(wclfh, cmdline, filename2)
                    self.update(wclobj2)
                    line = in_file.readline()
                    linecnt += 1
                    continue

                # handle calls to external functions to get more information usually from db
                patmatch = re.search(r"<<inclfunc ([^>]+)>>", line)
                if patmatch is not None:
                    if miscutils.fwdebug_check(9, "WCL_DEBUG"):
                        miscutils.fwdebug_print(f"patmatch={patmatch.group(0)}")
                    funcmatch = re.match(r'([^(]+)\(([^)]+)\)', patmatch.group(1))
                    if funcmatch:
                        if miscutils.fwdebug_check(9, "WCL_DEBUG"):
                            miscutils.fwdebug_print(f"funcmatch keys={funcmatch.group(2)}")
                            miscutils.fwdebug_print(f"funcmatch funcname={funcmatch.group(1)}")
                        keys = miscutils.fwsplit(funcmatch.group(2), ',')
                        argd = {}
                        for k in keys:
                            argd[k] = self.getfull(k)

                        p, m = funcmatch.group(1).rsplit('.', 1)
                        mod = import_module(p)
                        get_info_func = getattr(mod, m)
                        newinfo = get_info_func(argd)
                        self.update(newinfo)
                    else:
                        raise SyntaxError(f'File {filename} Line {linecnt:d} - Error:  Invalid inclfunc {patmatch}')

                    line = in_file.readline()
                    linecnt += 1
                    continue

                # handle group closing line <\string>
                pat_match = re.search(r"^\s*</\s*(\S+)\s*>\s*$", line)
                if pat_match is not None:
                    key = pat_match.group(1).lower()
                    if key in ['cmdline', 'replace']:
                        cmdline = False
                    sublabel = '__sublabel__' in curr
                    if sublabel:
                        del curr['__sublabel__']

                    if key == stackkeys[len(stackkeys)-1]:
                        stackkeys.pop()
                        stack.pop()
                        curr = stack[len(stack)-1]
                    elif sublabel:
                        if key == stackkeys[len(stackkeys) - 2]:
                            stackkeys.pop()
                            stack.pop()
                            curr = stack[len(stack)-1]

                            stackkeys.pop()
                            stack.pop()
                            curr = stack[len(stack)-1]
                        else:
                            print("******************************")
                            print("Linecnt =", linecnt)
                            print("Line =", line.strip())
                            print("Closing Key =", key)
                            self._print_stack(stackkeys, stack)

                            raise SyntaxError(f'File {filename} Line {linecnt:d} - Error:  Invalid or missing section' +
                                              f'close.   Got close for {key}. Expecting close for {stackkeys[len(stackkeys) - 2]}.')
                    else:
                        print("******************************")
                        print("Linecnt =", linecnt)
                        print("Line =", line.strip())
                        print("Closing Key =", key)
                        self._print_stack(stackkeys, stack)
                        raise SyntaxError(f'File {filename} Line {linecnt:d} - Error:  Invalid or missing section' +
                                          f'close.   Got close for {key}. Expecting close for {stackkeys[len(stackkeys) - 1]}.')

                    line = in_file.readline()
                    linecnt += 1
                    continue

                # handle group opening line <key sublabel> or <key>
                pat_match = re.search(r"^\s*<(\S+)\s*(\S+)?>\s*$", line)
                if pat_match is not None:
                    key = pat_match.group(1).lower()

                    # check for case where missing / when closing section
                    if key == stackkeys[-1]:
                        print("******************************")
                        print("Linecnt =", linecnt)
                        print("Line =", line.strip())
                        print("Opening Key =", key)
                        self._print_stack(stackkeys, stack)
                        raise SyntaxError(f'File {filename} Line {linecnt:d} - Error:  found ' +
                                          f'child section with same name ({key})')

                    stackkeys.append(key)

                    if not key in curr:
                        curr[key] = collections.OrderedDict()

                    stack.append(curr[key])
                    curr = curr[key]

                    if key in ['cmdline', 'replace']:
                        cmdline = True

                    if pat_match.group(2) is not None:
                        val = pat_match.group(2).lower()
                        stackkeys.append(val)
                        if not val in curr:
                            curr[val] = collections.OrderedDict()
                        curr[val]['__sublabel__'] = True
                        stack.append(curr[val])
                        curr = curr[val]
                    line = in_file.readline()
                    linecnt += 1
                    continue

                # handle key/val line: key = val
                pat_key_val = r"^\s*(\S+)(\s*=\s*)(.+)\s*$"
                pat_match = re.search(pat_key_val, line)
                if pat_match is not None:
                    key = pat_match.group(1)
                    if not cmdline:
                        key = key.lower()
                    curr[key] = pat_match.group(3).strip()
                    line = in_file.readline()
                    linecnt += 1
                    continue
                print("HERE")
                pat_key_val = r"^\s*(\S+)(\s+)([^=].*)\s*$"
                pat_match = re.search(pat_key_val, line)
                if pat_match is not None:
                    key = pat_match.group(1)
                    if not cmdline:
                        key = key.lower()
                    curr[key] = pat_match.group(3).strip()
                    line = in_file.readline()
                    linecnt += 1
                    continue

                print(f"Warning: Ignoring line #{linecnt:d} (did not match patterns):")
                print(line)

            # goto next line if no matches
            line = in_file.readline()
            linecnt += 1

        # done parsing input, should only be main dict in stack
        if len(stack) != 1 or len(stackkeys) != 1:
            self._print_stack(stackkeys, stack)
            print(f"File {filename} - Error parsing wcl_file.")
            print("Check that all sections have closing line.")
            raise SyntaxError(f"File {filename} - missing section closing line.")

    ############################################################
    def update(self, udict):
        """ update allowing for nested dictionaries """
        miscutils.updateOrderedDict(self, udict)

    ###########################################################################
    def getfull(self, key, opts=None, default=None):
        """ Return with variables replaced and expanded if string(s) """

        if miscutils.fwdebug_check(9, "WCL_DEBUG"):
            miscutils.fwdebug_print(f"BEG - key={key}")
            miscutils.fwdebug_print(f"default - {default}")
            miscutils.fwdebug_print(f"opts - {opts}")

        (found, value) = self.search(key, opts)
        if not found:
            value = default
        elif isinstance(value, str):
            if opts is None:
                newopts = {'expand': True,
                           intgdefs.REPLACE_VARS: True}
            else:
                newopts = copy.deepcopy(opts)

            if intgdefs.REPLACE_VARS not in newopts or \
               miscutils.convertBool(newopts[intgdefs.REPLACE_VARS]):
                newopts['expand'] = True
                if miscutils.fwdebug_check(9, "WCL_DEBUG"):
                    miscutils.fwdebug_print(f"calling replace_vars value={value}, opts={newopts}")

                (value, _) = replfuncs.replace_vars(value, self, newopts)
                if len(value) == 1:
                    value = value[0]

        return value

    ############################################################
    @classmethod
    def _print_stack(cls, stackkeys, stack):
        """ Print stackkeys and stack for debugging """
        print("----- STACK -----")
        if len(stackkeys) != len(stack):
            print("\tWarning: stackkeys and stack not same length")
        for i in range(0, max(len(stackkeys), len(stack))):
            skey = None
            stackinfo = None
            if i < len(stackkeys):
                skey = stackkeys[i]
            if i < len(stack):
                stackinfo = list(stack[i].keys())
            print(f"\t{i:d}: {skey} = {stackinfo}")
        print("\n\n")
