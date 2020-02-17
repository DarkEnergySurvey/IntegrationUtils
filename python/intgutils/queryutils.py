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
        miscutils.fwdebug_print(f"key = {key}")
        miscutils.fwdebug_print(f"value = {str(value)}")

    if ',' in value:
        value = value.replace(' ', '').split(',')

    condition = ""
    if isinstance(value, list):  # multiple values
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
        if ins:
            condition += f"{key} IN ({','.join(ins)})"
            if extra:
                condition += ' OR '

        if extra:
            condition += ' OR '.join(extra)

        if ' OR ' in condition:
            condition = f'({condition})'

        if nots:
            condition += ' AND ' + ' AND '.join(nots)

    elif '*' in value or '^' in value or '$' in value or '[' in value or ']' in value or '&' in value: # pragma: no cover
        condition = dbh.get_regexp_clause(key, value)
    elif '%' in value and '!' not in value:
        condition = f'{key} like {dbh.quote(value)}'
        if '\\' in value:
            condition += " ESCAPE '\\'"
    elif '%' in value and '!' in value:
        condition = f"{key} not like {dbh.quote(value.replace('!', ''))}"
        if '\\' in value:
            condition += " ESCAPE '\\'"
    elif '!' in value:
        value = value.replace('!', '')
        if value.lower() == 'null':
            condition = f"{key} is not NULL"
        else:
            condition = f"{key} != {dbh.quote(value)}"
    else:
        if value.lower() == 'null':
            condition = f"{key} is NULL"
        else:
            condition = f"{key} = {dbh.quote(value)}"

    return condition



###########################################################
# qdict[<table>][key_vals][<key>]
def create_query_string(dbh, qdict):
    """ returns a properly formatted sql query string given a special query dictionary  """

    selectfields = []
    fromtables = []
    whereclauses = []

    #print(qdict)

    for tablename, tabledict in qdict.items():
        fromtables.append(tablename)
        if 'select_fields' in tabledict:
            table_select_fields = tabledict['select_fields']
            if not isinstance(table_select_fields, list):
                table_select_fields = table_select_fields.lower().replace(' ', '').split(',')

            if 'all' in table_select_fields:
                selectfields.append(f"{tablename}.*")
            else:
                for field in table_select_fields:
                    selectfields.append(f"{tablename}.{field}")
        else:
            raise ValueError("Query dictionary must have 'select_fields' specified")
        if 'key_vals' in tabledict:
            for key, val in tabledict['key_vals'].items():
                whereclauses.append(make_where_clause(dbh, f'{tablename}.{key}', val))

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
                    whereclauses.append(f'{jtable}.{key}={val}')

    query = f"SELECT {','.join(selectfields)} FROM {','.join(fromtables)}"
    if whereclauses:
        query += f" WHERE {' AND '.join(whereclauses)}"
    return query


###########################################################
def gen_file_query(dbh, query, debug=3):
    """ Generic file query """

    sql = create_query_string(dbh, query)
    if debug >= 3:
        print("sql =", sql)

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
        print("gen_file_list: calling gen_file_query with", query)

    results = gen_file_query(dbh, query)

    if miscutils.fwdebug_check(1, 'PFWFILELIST_DEBUG'):
        miscutils.fwdebug_print(f"number of files in list from query = {len(results)}")

    if miscutils.fwdebug_check(3, 'PFWFILELIST_DEBUG'):
        miscutils.fwdebug_print(f"list from query = {results}")

    return results


###########################################################
def convert_single_files_to_lines(filelist, initcnt=1):
    """ Convert single files to dict of lines in prep for output """

    count = initcnt
    linedict = {'list': {}}

    if isinstance(filelist, dict) and len(filelist) > 1 and \
            'filename' not in filelist:
        filelist = list(filelist.values())
    elif isinstance(filelist, dict):  # single file
        filelist = [filelist]

    linedict = {'list': {intgdefs.LISTENTRY: {}}}
    for onefile in filelist:
        fname = f"file{count:05d}"
        lname = f"line{count:05d}"
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
        lname = f"line{lcnt:05d}"
        fsect = {}
        assert len(filelabels) == len(oneline)
        for fcnt, lab in enumerate(filelabels):
            fsect[lab] = oneline[fcnt]
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
        raise Exception(f'Invalid outtype ({outtype}).  Valid outtypes: xml, wcl, json')


###########################################################
def output_lines_xml(filename, dataset):
    """Writes dataset to file in XML format"""

    with open(filename, 'w') as xmlfh:
        xmlfh.write("<list>\n")
        for datak, line in dataset.items():
            xmlfh.write("\t<line>\n")
            for name, filedict in line.items():
                xmlfh.write(f"\t\t<file nickname='{name}'>\n")
                for key, val in filedict.items():
                    if key.lower() == 'ccd':
                        val = f"{val:02d}"
                    xmlfh.write(f"\t\t\t<{datak}>{val}</{datak}>")
                xmlfh.write(f"\t\t\t<fileid>{filedict['id']}</fileid>\n")
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
