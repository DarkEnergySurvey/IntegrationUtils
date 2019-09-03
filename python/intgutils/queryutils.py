#!/usr/bin/env python
# $Id: queryutils.py 41598 2016-04-05 20:26:20Z mgower $
# $Rev:: 41598                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-04-05 15:26:20 #$:  # Date of last commit.

""" Functions useful for query codes to be called by framework """

import re
import json

from intgutils.wcl import WCL
import intgutils.intgdefs as intgdefs
import despymisc.miscutils as miscutils

###########################################################
def make_where_clause(dbh, key, value):
    """ return properly formatted string for a where clause """

    if miscutils.fwdebug_check(1, 'PFWFILELIST_DEBUG'):
        miscutils.fwdebug_print("key = %s" % (key))
        miscutils.fwdebug_print("value = %s" % str(value))

    if ',' in value:
        value = value.replace(' ', '').split(',')

    condition = ""
    if type(value) is list:  # multiple values
        extra = []
        ins = []
        nots = []
        for val in value:
            if '%' in val:
                extra.append(make_where_clause(dbh, key, val))
            elif '!' in val:
                nots.append(make_where_clause(dbh, key, val))
            else:
                ins.append(dbh.quote(val))

        if len(ins) > 0:
            condition += "%s IN (%s)" % (key, ','.join(ins))
            if len(extra) > 0:
                condition += ' OR '

        if len(extra) > 0:
            condition += ' OR '.join(extra)

        if ' OR ' in condition:
            condition = '(%s)' % condition

        if len(nots) > 0:
            condition += ' AND '.join(nots)

    elif '*' in value or '^' in value or '$' in value or \
         '[' in value or ']' in value or '&' in value:
        condition = dbh.get_regexp_clause(key, value)
    elif '%' in value and '!' not in value:
        condition = '%s like %s' % (key, dbh.quote(value))
        if '\\' in value:
            condition += " ESCAPE '\\'"
    elif '%' in value and '!' in value:
        condition = '%s not like %s' % (key, dbh.quote(value))
        if '\\' in value:
            condition += " ESCAPE '\\'"
    elif '!' in value:
        if value.lower() == 'null':
            condition = "%s is not NULL" % key
        else:
            condition = '%s != %s' % (key, dbh.quote(value))
    else:
        if value.lower() == 'null':
            condition = "%s is NULL" % key
        else:
            condition = "%s = %s" % (key, dbh.quote(value))

    return condition



###########################################################
# qdict[<table>][key_vals][<key>]
def create_query_string(dbh, qdict):
    """ returns a properly formatted sql query string given a special query dictionary  """

    selectfields = []
    fromtables = []
    whereclauses = []

    print qdict

    for tablename, tabledict in qdict.items():
        fromtables.append(tablename)
        if 'select_fields' in tabledict:
            table_select_fields = tabledict['select_fields']
            if type(table_select_fields) is not list:
                table_select_fields = table_select_fields.lower().replace(' ', '').split(',')

            if 'all' in table_select_fields:
                selectfields.append("%s.*" % (tablename))
            else:
                for field in table_select_fields:
                    selectfields.append("%s.%s" % (tablename, field))

        if 'key_vals' in tabledict:
            for key, val in tabledict['key_vals'].items():
                whereclauses.append(make_where_clause(dbh, '%s.%s' % (tablename, key), val))

        if 'join' in tabledict:
            for j in tabledict['join'].lower().split(','):
                pat_key_val = r"^\s*([^=]+)(\s*=\s*)(.+)\s*$"
                pat_match = re.search(pat_key_val, j)
                if pat_match is not None:
                    key = pat_match.group(1)
                    if '.' in key:
                        (jtable, key) = key.split('.')
                    else:
                        jtable = tablename

                    val = pat_match.group(3).strip()
                    whereclauses.append('%s.%s=%s' % (jtable, key, val))

    query = "SELECT %s FROM %s WHERE %s" % \
                (','.join(selectfields),
                 ','.join(fromtables),
                 ' AND '.join(whereclauses))
    return query


###########################################################
def gen_file_query(dbh, query, debug=3):
    """ Generic file query """

    sql = create_query_string(dbh, query)
    if debug >= 3:
        print "sql =", sql

    curs = dbh.cursor()
    curs.execute(sql)
    desc = [d[0].lower() for d in curs.description]

    result = []
    for line in curs:
        linedict = dict(zip(desc, line))
        result.append(linedict)

    curs.close()
    return result


###########################################################
def gen_file_list(dbh, query, debug=3):
    """ Return list of files retrieved from the database using given query dict """

#    query['location']['key_vals']['archivesites'] = '[^N]'
#    query['location']['select_fields'] = 'all'
#    query['location']['hash_key'] = 'id'

    if debug:
        print "gen_file_list: calling gen_file_query with", query

    results = gen_file_query(dbh, query)

    if miscutils.fwdebug_check(1, 'PFWFILELIST_DEBUG'):
        miscutils.fwdebug_print("number of files in list from query = %s" % len(results))

    if miscutils.fwdebug_check(3, 'PFWFILELIST_DEBUG'):
        miscutils.fwdebug_print("list from query = %s" % results)

    return results


###########################################################
def convert_single_files_to_lines(filelist, initcnt=1):
    """ Convert single files to dict of lines in prep for output """

    count = initcnt
    linedict = {'list': {}}

    if type(filelist) is dict and len(filelist) > 1 and \
            'filename' not in filelist.keys():
        filelist = filelist.values()
    elif type(filelist) is dict:  # single file
        filelist = [filelist]

    linedict = {'list': {intgdefs.LISTENTRY: {}}}
    for onefile in filelist:
        fname = "file%05d" % (count)
        lname = "line%05d" % (count)
        linedict['list'][intgdefs.LISTENTRY][lname] = {'file': {fname: onefile}}
        count += 1
    return linedict

###########################################################
def convert_multiple_files_to_lines(filelist, filelabels, initcnt=1):
    """ Convert list of list of file dictionaries to dict of lines
        in prep for output for framework
        (filelist = [ [ {file 1 dict} {file 2 dict} ] [ { file 1 dict}..."""

    lcnt = initcnt
    lines = {'list': {intgdefs.LISTENTRY: {}}}
    for oneline in filelist:
        lname = "line%05d" % (lcnt)
        fsect = {}
        assert len(filelabels) == len(oneline)
        for fcnt in range(0, len(filelabels)):
            fsect[filelabels[fcnt]] = oneline[fcnt]
        lines['list'][intgdefs.LISTENTRY][lname] = {'file': fsect}
        lcnt += 1
    return lines

###########################################################
def output_lines(filename, dataset, outtype=intgdefs.DEFAULT_QUERY_OUTPUT_FORMAT):
    """ Writes dataset to file in specified output format """

    if outtype == 'xml':
        output_lines_xml(filename, dataset)
    elif outtype == 'wcl':
        output_lines_wcl(filename, dataset)
    elif outtype == 'json':
        output_lines_json(filename, dataset)
    else:
        raise Exception('Invalid outtype (%s).  Valid outtypes: xml, wcl, json' % outtype)


###########################################################
def output_lines_xml(filename, dataset):
    """Writes dataset to file in XML format"""

    with open(filename, 'w') as xmlfh:
        xmlfh.write("<list>\n")
        for datak, line in dataset.items():
            xmlfh.write("\t<line>\n")
            for name, filedict in line.items():
                xmlfh.write("\t\t<file nickname='%s'>\n" % name)
                for key, val in filedict.items():
                    if key.lower() == 'ccd':
                        val = "%02d" % (val)
                    xmlfh.write("\t\t\t<%s>%s</%s>" % (datak, val, datak))
                xmlfh.write("\t\t\t<fileid>%s</fileid>\n" % (filedict['id']))
                xmlfh.write("\t\t</file>\n")
            xmlfh.write("\t</line>\n")
        xmlfh.write("</list>\n")


###########################################################
def output_lines_wcl(filename, dataset):
    """ Writes dataset to file in WCL format """

    dswcl = WCL(dataset)
    with open(filename, "w") as wclfh:
        dswcl.write(wclfh, True, 4)  # print it sorted


###########################################################
def output_lines_json(filename, dataset):

    """ Writes dataset to file in json format """
    with open(filename, "w") as jsonfh:
        json.dump(dataset, jsonfh, indent=4, separators=(',', ': '))
