#!/usr/bin/env python3

import unittest
import os
import stat
import sys
import copy
import time
from contextlib import contextmanager
from io import StringIO
from mock import patch
import json

import intgutils.intgmisc as igm
import intgutils.replace_funcs as rf
import intgutils.intgdefs as intgdefs
import intgutils.wcl as wcl
import intgutils.queryutils as iqu
import intgutils.basic_wrapper as bwr

import despydmdb.desdmdbi as dmdbi
from MockDBI import MockConnection


ROOT = '/var/lib/jenkins/test_data/'

@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

def wclDiff(f1, f2, ignore_outer_whitespace=False, ignore_blank_lines=False):
    data = [[], []]
    for line in open(f1, 'r').readlines():
        line = line.rstrip()
        if ignore_outer_whitespace:
            line = line.lstrip()
        if ignore_blank_lines:
            if not line:
                continue
        data[0].append(line)

    for line in open(f2, 'r').readlines():
        line = line.rstrip()
        if ignore_outer_whitespace:
            line = line.lstrip()
        if ignore_blank_lines:
            if not line:
                continue
        data[1].append(line)

    # test length
    if len(data[0]) != len(data[1]):
        return False
    for i in range(len(data[0])):
        if data[0][i] != data[1][i]:
            return False
    return True

def wcl_to_dict(w):
    d = {}
    for k, v in w.items():
        if isinstance(v, dict):
            d[k] = wcl_to_dict(v)
        else:
            d[k] = copy.deepcopy(v)
    return d

class TestIntgmisc(unittest.TestCase):
    wcl_file = os.path.join(ROOT, 'wcl/TEST_DATA_r15p03_full_config.des')

    def test_check_files(self):
        files = [ROOT + 'raw/test_raw.fits.fz', ROOT + 'raw/notthere.fits']
        (exist, missing) = igm.check_files(files)
        self.assertEqual(len(exist), 1)
        self.assertEqual(len(missing), 1)
        self.assertTrue('test_raw.fits' in exist[0])
        self.assertTrue('notthere' in missing[0])

    def test_get_cmd_hyphen(self):
        self.assertEqual('--', igm.get_cmd_hyphen('alldouble', 'test'))
        self.assertEqual('-', igm.get_cmd_hyphen('allsingle', 'test'))
        self.assertEqual('--', igm.get_cmd_hyphen('mixed_gnu', 'test'))
        self.assertEqual('-', igm.get_cmd_hyphen('mixed_gnu', 't'))
        self.assertRaises(ValueError, igm.get_cmd_hyphen, 'blah', 'test')

    def test_get_exec_sections(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        res = igm.get_exec_sections(w['module']['band-assemble'], 'exec_')
        keys = list(res.keys())
        self.assertEqual(1, len(keys))
        self.assertEqual('exec_1', keys[0])
        self.assertEqual('file.coadd', res['exec_1']['was_generated_by'])

    def test_run_exec(self):
        (retcode, procinfo) = igm.run_exec('ls')
        self.assertEqual(retcode, 0)
        self.assertTrue(procinfo['ru_stime'] >= 0.)

    def test_remove_column_format(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        res = igm.remove_column_format(list(w['module']['band-swarp-msk-nobkg']['list']['imgme_nobkg']['div_list_by_col']['sci_nobkg'].values()))
        self.assertEqual(7, len(res))
        self.assertTrue('nwgint.fullname' in res)
        for r in res:
            self.assertFalse('FMT' in r)

        with capture_output() as (out, _):
            with patch('intgutils.intgmisc.re.match', side_effect=[None]):
                self.assertRaises(SystemExit, igm.remove_column_format, list(w['module']['band-swarp-msk-nobkg']['list']['imgme_nobkg']['div_list_by_col']['sci_nobkg'].values()))
            output = out.getvalue().strip()
            self.assertTrue('invalid' in output)

    def test_convert_col_string_to_list(self):
        inp = 'a,b,c,d'
        res = igm.convert_col_string_to_list(inp)
        self.assertEqual(4, len(res))
        self.assertTrue('b' in res)
        res = igm.convert_col_string_to_list(inp, False)
        self.assertEqual(4, len(res))
        self.assertEqual('b', res[1])

    def test_read_fullnames_from_listfile(self):
        fname = 'list/mangle/DES2157-5248_r15p03_g_mangle-red.list'
        os.symlink(os.path.join(ROOT, 'list'), 'list', target_is_directory=True)
        try:
            w = wcl.WCL()
            with open(self.wcl_file, 'r') as infh:
                w.read(infh, self.wcl_file)
            columns = w['module']['mangle']['list']['red']['columns']
            res = igm.read_fullnames_from_listfile(fname, 'textcsv', columns)
            self.assertEqual(len(res), 1)
            self.assertTrue('red_immask' in res.keys())
            self.assertEqual(len(res['red_immask']), 169)
            self.assertFalse(',' in res['red_immask'][25])

            res = igm.read_fullnames_from_listfile(fname, 'texttab', columns)
            self.assertEqual(len(res), 1)
            self.assertTrue('red_immask' in res.keys())
            self.assertEqual(len(res['red_immask']), 169)
            self.assertTrue(',' in res['red_immask'][25])
            self.assertFalse(res['red_immask'][25].endswith(','))

            res = igm.read_fullnames_from_listfile(fname, 'textsp', columns)
            self.assertEqual(len(res), 1)
            self.assertTrue('red_immask' in res.keys())
            self.assertEqual(len(res['red_immask']), 169)
            self.assertTrue(',' in res['red_immask'][25])
            self.assertTrue(res['red_immask'][25].endswith(','))

            with capture_output() as (out, _):
                self.assertRaises(SystemExit, igm.read_fullnames_from_listfile, fname, 'wcl', columns)
                output = out.getvalue().strip()
                self.assertTrue('supported' in output)

            with capture_output() as (out, _):
                self.assertRaises(SystemExit, igm.read_fullnames_from_listfile, fname, 'unk', columns)
                output = out.getvalue().strip()
                self.assertTrue('unknown' in output)
        finally:
            try:
                os.unlink('list')
            except:
                pass

    def test_get_list_fullnames(self):
        fname = 'list/mangle/DES2157-5248_r15p03_g_mangle-red.list'
        os.symlink(os.path.join(ROOT, 'list'), 'list', target_is_directory=True)
        try:
            w_file = os.path.join(ROOT, 'wcl/DES2157-5248_r15p03_g_mangle_input.wcl')
            w = wcl.WCL()
            with open(w_file, 'r') as infh:
                w.read(infh, w_file)
            name, fnames = igm.get_list_fullnames('cmdline.red.red_immask', w)
            self.assertEqual('list/mangle/DES2157-5248_r15p03_g_mangle-red.list', name)
            self.assertEqual(169, len(fnames))
            self.assertTrue('red/D00791642_g_c33_r4055p01_immasked.fits.fz' in fnames)

            with capture_output() as (out, _):
                self.assertRaises(IOError, igm.get_list_fullnames, 'cmdline.red-fail.red_immask', w)
                output = out.getvalue().strip()
                self.assertTrue('does not exist' in output)

            with capture_output() as (out, _):
                name, fnames = igm.get_list_fullnames('cmdline.red-test.red_immask', w)
                self.assertEqual(len(fnames), 0)
                output = out.getvalue().strip()
                self.assertTrue('ERROR: Could not' in output)

            with capture_output() as (out, _):
                name, fnames = igm.get_list_fullnames('cmdline.red-test2.red_immask', w)
                self.assertEqual(len(fnames), 0)
                output = out.getvalue().strip()
                self.assertTrue('ERROR: Could not' not in output)
        finally:
            try:
                os.unlink('list')
            except:
                pass

    def test_get_file_fullnames(self):
        #fname = 'list/mangle/DES2157-5248_r15p03_g_mangle-red.list'
        os.symlink(os.path.join(ROOT, 'list'), 'list', target_is_directory=True)
        try:
            w_file = os.path.join(ROOT, 'wcl/DES2157-5248_r15p03_g_mangle_input.wcl')
            w = wcl.WCL()
            with open(w_file, 'r') as infh:
                w.read(infh, w_file)
            fw = w['filespecs']
            res = igm.get_file_fullnames('filespecs.polygons', fw, w)
            self.assertEqual(len(res), 6)

            res = igm.get_file_fullnames('filespecs.polygons2', fw, w)
            self.assertEqual(len(res), 0)

            res = igm.get_file_fullnames('filespecs.polytiles', fw, w)
            self.assertEqual(len(res), 0)

        finally:
            try:
                os.unlink('list')
            except:
                pass

    def test_get_fullnames(self):
        os.symlink(os.path.join(ROOT, 'list'), 'list', target_is_directory=True)
        try:
            f = open('list/mangle/DES2157-5248_r15p03_g_mangle-out.list', 'w')
            f.write('\n')
            f.close()
            w_file = os.path.join(ROOT, 'wcl/DES2157-5248_r15p03_g_mangle_input.wcl')
            w = wcl.WCL()
            with open(w_file, 'r') as infh:
                w.read(infh, w_file)
            full_file = os.path.join(ROOT, 'wcl/TEST_DATA_r15p03_full_config.des')
            fw = wcl.WCL()
            with open(full_file, 'r') as infh:
                fw.read(infh, full_file)

            i, o = igm.get_fullnames(w, fw, 'exec_1')
            self.assertEqual(len(i.keys()), 7)
            self.assertTrue('filespecs.poltolys' in i)
            self.assertEqual(len(i['list.nwgint.nwgint']), 169)

            self.assertEqual(len(o.keys()), 9)
            self.assertTrue('filespecs.molys' in o)
            self.assertEqual(len(o['filespecs.molys']), 6)

            with capture_output() as (out, _):
                self.assertRaises(KeyError, igm.get_fullnames, w, fw,'exec_2')
                output = out.getvalue().strip()
                self.assertTrue('sectkeys' in output)

            with capture_output() as (out, _):
                self.assertRaises(KeyError, igm.get_fullnames, w, fw, 'exec_3')
                output = out.getvalue().strip()
                self.assertTrue('sectkeys' in output)

            with capture_output() as (out, _):
                self.assertRaises(KeyError, igm.get_fullnames, w, fw)
                output = out.getvalue().strip()
                self.assertTrue('sectkeys' in output)

        finally:
            try:
                os.unlink('list/mangle/DES2157-5248_r15p03_g_mangle-out.list')
                os.unlink('list')
            except:
                pass

    def test_check_input_files(self):
        fname = 'list/mangle/DES2157-5248_r15p03_g_mangle-red.list'
        os.symlink(os.path.join(ROOT, 'list'), 'list', target_is_directory=True)
        try:
            w_file = os.path.join(ROOT, 'wcl/DES2157-5248_r15p03_g_mangle_input.wcl')
            w = wcl.WCL()
            with open(w_file, 'r') as infh:
                w.read(infh, w_file)
            fw = w['filespecs']
            exist, miss = igm.check_input_files('cmdline.coadd', fw)
            self.assertEqual(len(exist), 0)
            self.assertEqual(len(miss), 1)
        finally:
            try:
                os.unlink('list')
            except:
                pass


class TestReplaceFuncs(unittest.TestCase):
    @classmethod
    def setUp(cls):
        wcl_file = os.path.join(ROOT, 'wcl/DES2157-5248_r15p03_g_mangle_input.wcl')
        cls.w = wcl.WCL()
        with open(wcl_file, 'r') as infh:
            cls.w.read(infh, wcl_file)
        full_file = os.path.join(ROOT, 'wcl/TEST_DATA_r15p03_full_config.des')
        cls.fw = wcl.WCL()
        with open(full_file, 'r') as infh:
            cls.fw.read(infh, full_file)


    def test_replace_vars_type_wcl(self):
        done, res, data = rf.replace_vars_type(self.w['exec_1']['cmdline']['molysprefix'],
                                               self.w, False, '')
        self.assertFalse(done)
        self.assertEqual(res, 'mangle/g/TEST_DATA_r15p03_g_molys')
        self.assertEqual(data['band'], 'g')
        self.assertEqual(data['reqnum'], '15')

    def test_replace_vars_type_header(self):
        done, res, data = rf.replace_vars_type(f'$HEAD{{{ROOT}/cat/test_g_cat.fits,ORIGIN}}',
                                               self.w, True, 'HEAD')
        self.assertEqual('SExtractor', res)

    def test_replace_vars_type_func(self):
        done, res, data = rf.replace_vars_type("$FUNC{tester.add,1,2,3}", self.w, True, 'FUNC')
        self.assertEqual(res, '6')

    def test_replace_vars_type_other(self):
        done, res, data = rf.replace_vars_type("${band}", {'band': 'e'}, False, '')
        self.assertEqual(res, 'e')

        done, res, data = rf.replace_vars_type("${band}", {'bands': 'e'}, False, '')
        self.assertEqual(res, '')

        self.assertRaises(KeyError, rf.replace_vars_type, "${band}", {'bands': 'e'}, True, '')

        with patch('intgutils.replace_funcs.replace_vars_single', side_effect=TypeError):
            with capture_output() as (out, _):
                self.assertRaises(TypeError, rf.replace_vars_type,
                                  self.w['exec_1']['cmdline']['molysprefix'] + ":var",
                                  self.w, False, '')
                output = out.getvalue().strip()
                self.assertTrue("prpat" in output)

    def test_replace_vars_type_expand(self):
        done, res, data = rf.replace_vars_type("${band}", {'band': '(e)'}, False, '',
                                               {'expand': True})
        self.assertTrue('LOOP' in res)

        done, res, data = rf.replace_vars_type("${band}", {'band': '(e)'}, False, '',
                                               {'expand': False})
        self.assertEqual('(e)', res)

        done, res, data = rf.replace_vars_type("${band}", {'band': '(e)'}, False, '',
                                               {'x': 'y'})
        self.assertEqual('(e)', res)

    def test_replace_vars_loop_basic(self):
        vals, keep = rf.replace_vars_loop(('$LOOP{bands}', {}), self.fw)
        self.assertEqual(len(vals), 5)
        self.assertTrue('g' in vals)
        self.assertTrue(len(vals) == len(keep))
        self.assertTrue(keep[0]['bands'] in vals)

    def test_replace_vars_loop_pad(self):
        vals, keep = rf.replace_vars_loop(('$LOOP{ccds:02}',{}), self.fw)
        self.assertEqual(len(vals), 5)
        self.assertTrue('01' in vals)
        self.assertTrue(len(vals) == len(keep))
        self.assertTrue('0' + keep[0]['ccds'] in vals)

    def test_replace_vars_loop_double(self):
        vals, keep = rf.replace_vars_loop(('$LOOP{band_ccd}',{}), self.fw)
        self.assertEqual(len(vals), 10)
        self.assertTrue('b1' in vals)
        self.assertTrue(len(vals) == len(keep))

    def test_replace_vars(self):
        res, data = rf.replace_vars(self.fw['ops_run_dir'], self.fw)
        self.assertTrue('ACT/multi' in res)
        self.assertTrue('project' in data)
        self.assertEqual('ACT', data['project'])

    def test_replace_vars_infinite(self):
        self.assertRaises(Exception, rf.replace_vars, self.fw['infinite'], self.fw)

        self.assertRaises(Exception, rf.replace_vars, '$FUNC{tester.infinite,1,2}', self.fw)

    def test_replace_vars_loop(self):
        res, data = rf.replace_vars(self.fw['band_ccd'], self.fw)
        self.assertEqual(len(res), 5)

        res, data = rf.replace_vars(self.fw['band_ccd'], self.fw, {'opt': 1})
        self.assertEqual(len(res), 5)

    def test_replace_vars_single(self):
        self.assertTrue('ACT' in rf.replace_vars_single(self.fw['ops_run_dir'], self.fw))

        with capture_output() as (out, _):
            self.assertRaises(KeyError, rf.replace_vars_single, self.fw['band_ccd'], self.fw)
            output = out.getvalue().strip()
            self.assertTrue('Error:  Multiple' in output)

        self.assertEqual(rf.replace_vars_single(self.fw['single_loop'], self.fw), '2868742')

class TestWCL(unittest.TestCase):
    wcl_file = ROOT + 'wcl/TEST_DATA_r15p03_full_config.des'
    def test_init(self):
        try:
            os.unlink('out.wcl')
        except:
            pass

        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        self.assertEqual(w['attnum'], str(3))
        w.write(open('out.wcl', 'w'))
        self.assertTrue(wclDiff(self.wcl_file, 'out.wcl', ignore_blank_lines=True))
        os.unlink('out.wcl')

    def test_set_search_order(self):
        w = wcl.WCL()
        srch_order = None
        self.assertIsNotNone(w.search_order)
        w.set_search_order(srch_order)
        self.assertIsNone(w.search_order)

    def test_get(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        self.assertIsNone(w.get('notthere'))
        self.assertEqual(w.get('attnum'), str(3))

    def test_set(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        self.assertEqual(w.get('attnum'), str(3))
        w.set('attnum', '15')
        self.assertEqual(w.get('attnum'), str(15))

        self.assertEqual(w.get('directory_pattern.inputwcl.name'), 'wcl')
        w.set('directory_pattern.inputwcl.name', 'bob')
        self.assertEqual(w.get('directory_pattern.inputwcl.name'), 'bob')

    def test_search_basic(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        res = w.search('directory_pattern.inputwcl')

        self.assertTrue(res[0])
        self.assertEqual(res[1]['name'], 'wcl')

        res2 = w.search('dirn')
        self.assertFalse(res2[0])

        self.assertEqual(w.search('directory_pattern.inputwcl2')[1], '')

    def test_search_opts(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)

        res = w.search('filename_pattern')

        res2 = w.search('filename_pattern', {'currentvals': {'runjob': 'rnj.sh'}})

        self.assertTrue(res2[0])
        self.assertEqual(res2[1]['runjob'], 'runjob.sh')
        self.assertEqual(res, res2)

    def test_search_error(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)

        with capture_output() as (out, _):
            w.search(2)
            output = out.getvalue().strip()
            self.assertTrue('key' in output)

        self.assertFalse(w.search('a,ops_run_dir')[0])


    def test_search_current(self):
        wcl_file = os.path.join(ROOT, 'wcl/DES2157-5248_r15p03_g_mangle_input.wcl')
        w2 = wcl.WCL()
        with open(wcl_file, 'r') as infh:
            w2.read(infh, wcl_file)

        self.assertEqual(w2.search('runsite')[1], 'somewhere')

        self.assertEqual(w2.search('runsite', {'currentvals':{'runsite': 'nowhere'}})[1], 'nowhere')

    def test_search_required(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)

        item = 'ops_out_dir'
        w.search(item)
        with capture_output() as (out, _):
            self.assertRaises(KeyError, w.search, item, {'required': True})
            output = out.getvalue().strip()
            self.assertTrue(item in output)
            self.assertTrue('Error' in output)

    def test_search_searchobj(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)

        self.assertFalse(w.search(2)[0])

        self.assertEquals(4, w.search(2, {'searchobj': {2: 4}})[1])

        self.assertFalse(w.search(2, {'opt': 2})[0])

        self.assertFalse(w.search(2, {'searchobj': {4: 2}})[0])


    def test_search_order(self):
        SEARCH_ORDER = ['file', 'list', 'exec', 'job', 'module', 'block', 'archive', 'site']
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        w.set_search_order(SEARCH_ORDER)
        self.assertTrue(w.search(2)[1] == '')
        done, res = w.search('blockname')
        self.assertEqual(res, 'meds')


    def test_search_wcl_for_variables(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)

        vars = wcl.WCL.search_wcl_for_variables(w)
        self.assertTrue(vars['file.det_segmap.fullname'])

    def test_write(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        self.assertEqual(w['attnum'], str(3))
        w.write(open('out.wcl', 'w'))
        self.assertTrue(wclDiff(self.wcl_file, 'out.wcl', ignore_blank_lines=True))

        w.write(open('out.wcl', 'w'), indent=2)
        self.assertFalse(wclDiff(self.wcl_file, 'out.wcl', ignore_blank_lines=True))
        self.assertTrue(wclDiff(self.wcl_file, 'out.wcl', ignore_outer_whitespace=True,
                                ignore_blank_lines=True))

        w.write(open('out.wcl', 'w'), sortit=True, indent=2)
        self.assertFalse(wclDiff(self.wcl_file, 'out.wcl', ignore_blank_lines=True))
        self.assertFalse(wclDiff(self.wcl_file, 'out.wcl', ignore_outer_whitespace=True,
                                ignore_blank_lines=True))

        with capture_output() as (out, _):
            w.write()
            output = out.getvalue().strip()
            self.assertTrue('attnum = 3' in output)

        w.set('attnum', '15')
        w.write(open('out.wcl', 'w'))
        self.assertFalse(wclDiff(self.wcl_file, 'out.wcl', ignore_blank_lines=True))

        os.unlink('out.wcl')

    def test_read(self):
        wfl = os.path.join(ROOT, 'wcl/test.wcl')
        w = wcl.WCL()
        with capture_output() as (out, _):
            with open(wfl, 'r') as infh:
                w.read(infh, wfl)
            output = out.getvalue().strip()
            self.assertTrue('Ignoring' in output)
        self.assertTrue('exec_1' in w)

        self.assertEqual(6, int(w['myval']))

        self.assertEqual(7, int(w['now']))

    def test_read_error(self):
        wfl = os.path.join(ROOT, 'wcl/bad.wcl')
        w = wcl.WCL()
        infh = open(wfl, 'r')
        self.assertRaises(SyntaxError, w.read, infh, wfl)
        infh.close()
        del w

        with capture_output() as (out, _):
            wfl = os.path.join(ROOT, 'wcl/bad1.wcl')
            w = wcl.WCL()
            infh = open(wfl, 'r')
            self.assertRaises(SyntaxError, w.read, infh, wfl)
            infh.close()
            output = out.getvalue().strip()
            self.assertTrue('myspecs' in output)
            del w

        with capture_output() as (out, _):
            wfl = os.path.join(ROOT, 'wcl/bad2.wcl')
            w = wcl.WCL()
            infh = open(wfl, 'r')
            self.assertRaises(SyntaxError, w.read, infh, wfl)
            infh.close()
            output = out.getvalue().strip()
            self.assertTrue('subspec' in output)
            del w

        with capture_output() as (out, _):
            wfl = os.path.join(ROOT, 'wcl/bad3.wcl')
            w = wcl.WCL()
            infh = open(wfl, 'r')
            self.assertRaises(SyntaxError, w.read, infh, wfl)
            infh.close()
            output = out.getvalue().strip()
            self.assertTrue('myspecs' in output)

        with capture_output() as (out, _):
            wfl = os.path.join(ROOT, 'wcl/bad4.wcl')
            w = wcl.WCL()
            infh = open(wfl, 'r')
            self.assertRaises(SyntaxError, w.read, infh, wfl)
            infh.close()
            output = out.getvalue().strip()
            self.assertTrue('myspecs' in output)

    def test_print(self):
        stack = [{'start': 'a', 'end': 'b'}, {'top': 1, 'bottom':4}]
        keys = ['first', 'second']
        w = wcl.WCL()
        with capture_output() as (out, _):
            w._print_stack(keys, stack)
            output = out.getvalue().strip()
            self.assertTrue('second' in output)
            self.assertTrue('start' in output)

        keys = ['first']
        with capture_output() as (out, _):
            w._print_stack(keys, stack)
            output = out.getvalue().strip()
            self.assertTrue('first' in output)
            self.assertTrue('start' in output)
            self.assertFalse('second' in output)
            self.assertTrue('Warning' in output)


    def test_getfull(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)

        self.assertIsNone(w.getfull('this_is_missing', default=None))
        self.assertTrue(isinstance(w.getfull('module'), dict))
        val = w.getfull('ops_run_dir')
        val1 = w.getfull('ops_run_dir', opts={})
        val2 = w.getfull('ops_run_dir', opts={intgdefs.REPLACE_VARS: False})
        self.assertEqual(val, val1)
        self.assertNotEqual(val, val2)
        self.assertTrue('$' in val2)
        self.assertFalse('$' in val)

class TestQueryUtils(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-maximal]
PASSWD  =   maximal_passwd
name    =   maximal_name_1    ; if repeated last name wins
user    =   maximal_name      ; if repeated key, last one wins
Sid     =   maximal_sid       ;comment glued onto value not allowed
type    =   POSTgres
server  =   maximal_server

[db-minimal]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   oracle

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))
        cls.dbh = dmdbi.DesDmDbi(cls.sfile, 'db-test')

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.sfile)
        #MockConnection.destroy()

    def test_make_where_clause(self):
        key = 'hello'
        val = 'bye'
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue(key in res)
        self.assertTrue(val in res)
        self.assertTrue('=' in res)

    def test_make_where_clause_like(self):
        key = 'hello'
        val = '%bye'
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue(key in res)
        self.assertTrue(val in res)
        self.assertTrue('like' in res)

        val = '\\%bye'
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue('ESCAPE' in res)

        val = '\\!%bye'
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue('ESCAPE' in res)

        val = "!%bye"
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue('not like' in res)

    def test_where_clause_not(self):
        key = 'hello'
        val = '!bye'
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue(key in res)
        self.assertTrue(val.replace('!', '') in res)
        self.assertTrue('!=' in res)


    def test_make_where_clause_null(self):
        key = 'hello'
        val = "!nUll"
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue('is not NULL' in res)

        val = 'NuLl'
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue('is NULL' in res)

    def test_where_clause_list(self):
        key = 'hello'
        val = ['bye,up,%good,!bad']
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue(' IN ' in res)
        self.assertTrue(' OR ' in res)
        self.assertTrue(' AND ' in res)
        self.assertTrue('!=' in res)

        val = ['%bye']
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue(key in res)
        self.assertTrue(val[0] in res)
        self.assertTrue('like' in res)

        val = ['!bye']
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue(key in res)
        self.assertTrue(val[0].replace('!', '') in res)
        self.assertTrue('!=' in res)

        val = ['bye']
        res = iqu.make_where_clause(self.dbh, key, val)
        self.assertTrue(key in res)
        self.assertTrue(val[0] in res)

    def test_create_query_string(self):
        a = {'tname': {'select_fields': ['fld'],
                       'key_vals': {'b':'5'}}}
        res = iqu.create_query_string(self.dbh, a)
        self.assertTrue('tname.fld' in res)
        self.assertTrue('tname.b =' in res)

        a = {'tname': {'select_fields': 'all'}}
        res = iqu.create_query_string(self.dbh, a)
        self.assertTrue('tname.*' in res)
        self.assertFalse('WHERE' in res)

        a = {'tname': {'select_fields': 'fld',
                       'join':'fld=5,other=bye'}}
        res = iqu.create_query_string(self.dbh, a)
        self.assertTrue('tname.fld' in res)
        self.assertTrue('tname.other=bye' in res)

        a = {'tname': {'select_fields': 'fld',
                       'key_vals':{'fld':'5'}},
             'bname': {'select_fields': 'other'}
             }
        res = iqu.create_query_string(self.dbh, a)
        self.assertTrue('tname.fld' in res)
        self.assertTrue('bname.other' in res)
        self.assertTrue('tname,bname' in res)

        a = {'tname': {'join':'fld=5,other=bye'}}
        self.assertRaises(ValueError, iqu.create_query_string, self.dbh, a)

        a = {'tname': {'select_fields': 'fld',
                       'join':'other.fld=5,other=bye'}}
        res = iqu.create_query_string(self.dbh, a)
        self.assertTrue('tname.fld' in res)
        self.assertTrue('other.fld=' in res)

        a = {'tname': {'select_fields': 'fld',
                       'join':'other'}}
        res = iqu.create_query_string(self.dbh, a)
        self.assertTrue('tname.fld' in res)
        self.assertFalse('other' in res)

    def test_gen_file_query(self):
        qry = {'catalog': {'select_fields': 'filename',
                           'key_vals': {'band': 'g'}}}
        with capture_output() as (out, _):
            res = iqu.gen_file_query(self.dbh, qry)
            self.assertEqual(1394, len(res))
            output = out.getvalue().strip()
            self.assertTrue('sql =' in output)

        with capture_output() as (out, _):
            res = iqu.gen_file_query(self.dbh, qry, 2)
            self.assertEqual(1394, len(res))
            output = out.getvalue().strip()
            self.assertFalse('sql =' in output)

    def test_gen_file_list(self):
        qry = {'catalog': {'select_fields': 'filename',
                           'key_vals': {'band': 'g'}}}
        with capture_output() as (out, _):
            res = iqu.gen_file_list(self.dbh, qry)
            self.assertEqual(1394, len(res))
            output = out.getvalue().strip()
            self.assertTrue('gen_file_list' in output)

        with capture_output() as (out, _):
            res = iqu.gen_file_list(self.dbh, qry, 2)
            self.assertEqual(1394, len(res))
            output = out.getvalue().strip()
            self.assertFalse('gen_file_list' in output)

    def test_convert_single_files_to_lines(self):
        flist = {'file1': 'first.file',
                 'file2': 'second.file',
                 'file3': 'third.file'}
        expected = {'list': {'line': {'line00001': {'file': {'file00001': 'first.file'}},
                                      'line00002': {'file': {'file00002': 'second.file'}},
                                      'line00003': {'file': {'file00003': 'third.file'}}}}}

        res = iqu.convert_single_files_to_lines(flist)
        self.assertDictEqual(res, expected)

        expected = {'list': {'line': {'line00001': {'file': {'file00001': {'file1': 'first.file'}}}}}}
        flist = {'file1': 'first.file'}
        res = iqu.convert_single_files_to_lines(flist)
        self.assertDictEqual(res, expected)

        flist = ['first.file', 'second.file']
        expected = {'list': {'line': {'line00001': {'file': {'file00001': 'first.file'}},
                                      'line00002': {'file': {'file00002': 'second.file'}}}}}
        res = iqu.convert_single_files_to_lines(flist)
        self.assertDictEqual(res, expected)

    def test_convert_multiple_files_to_lines(self):
        flist = [[{'file00001': 'first.file'},
                  {'file00002': 'second.file'}],
                 [{'file00003': 'third.file'},
                  {'file00004': 'fourth.file'}]]
        expected = {'list': {'line': {'line00001': {'file': {'one': {'file00001': 'first.file'},
                                                             'two': {'file00002': 'second.file'}}},
                                      'line00002': {'file': {'one': {'file00003': 'third.file'},
                                                             'two': {'file00004': 'fourth.file'}}}}}}

        res = iqu.convert_multiple_files_to_lines(flist, ['one', 'two'])
        self.assertDictEqual(res, expected)

    def test_output_lines_xml(self):
        fname = 'test.xml'
        try:
            os.unlink(fname)
        except:
            pass
        try:
            expected = {'list': {'line': {'line00001': {'file': {'one': {'file00001': 'first.file'},
                                                                 'two': {'file00002': 'second.file'}}},
                                          'id': '456',
                                          'line00002': {'file': {'one': {'file00003': 'third.file'},
                                                                 'two': {'file00004': 'fourth.file'}}
                                                        }}}}
            iqu.output_lines(fname, expected, 'xml')

            lines = open(fname, 'r').readlines()
            items = {'file00001': False,
                     'third.file': False}
            for line in lines:
                for key in items.keys():
                    if key in line:
                        items[key] = True
            for key, val in items.items():
                self.assertTrue(val)

        finally:
            try:
                os.unlink(fname)
            except:
                pass


    def test_output_lines_wcl(self):
        fname = 'test.wcl'
        try:
            os.unlink(fname)
        except:
            pass
        try:
            expected = {'list': {'line': {'line00001': {'file': {'one': {'file00001': 'first.file'},
                                                                 'two': {'file00002': 'second.file'}}},
                                          'id': '456',
                                          'line00002': {'file': {'one': {'file00003': 'third.file'},
                                                                 'two': {'file00004': 'fourth.file'}}
                                                        }}}}
            iqu.output_lines(fname, expected, 'wcl')
            w = wcl.WCL()
            with open(fname, 'r') as infh:
                w.read(infh, fname)
            d = wcl_to_dict(w)
            self.assertDictEqual(expected, d)
        finally:
            try:
                os.unlink(fname)
            except:
                pass

    def test_output_lines_json(self):
        fname = 'test.json'
        try:
            os.unlink(fname)
        except:
            pass
        try:
            expected = {'list': {'line': {'line00001': {'file': {'one': {'file00001': 'first.file'},
                                                                 'two': {'file00002': 'second.file'}}},
                                          'id': '456',
                                          'line00002': {'file': {'one': {'file00003': 'third.file'},
                                                                 'two': {'file00004': 'fourth.file'}}
                                                        }}}}
            iqu.output_lines(fname, expected, 'json')
            with open(fname) as jfile:
                d = json.load(jfile)
            self.assertDictEqual(d, expected)
        finally:
            try:
                os.unlink(fname)
            except:
                pass



    def test_output_lines_error(self):
        expected = {'list': {'line': {'line00001': {'file': {'one': {'file00001': 'first.file'},
                                                             'two': {'file00002': 'second.file'}}},
                                      'line00002': {'file': {'one': {'file00003': 'third.file'},
                                                             'two': {'file00004': 'fourth.file'}}}}}}
        self.assertRaises(Exception, iqu.output_lines, 'blah.xml', expected, 'txt')

class TestBasicWrapper(unittest.TestCase):
    wcl_file = os.path.join(ROOT, 'wcl/wrappertest.wcl')

    def test_init(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        self.assertTrue('exec_1' in wr.inputwcl.keys())

    def test_determine_status(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        with capture_output() as (out, _):
            wr.run_wrapper()
            output = out.getvalue().strip()
            self.assertTrue('does not exist' in output)
        self.assertEqual(wr.determine_status(), 1)

    def test_start_exec_task(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        name = 'myexec'
        for ekey, iw_exec in sorted(execs.items()):
            ow_exec = {'task_info': {}}
            wr.outputwcl[ekey] = ow_exec
            wr.curr_exec = ow_exec
            self.assertFalse(name in wr.curr_exec['task_info'])
            wr.start_exec_task(name)
            self.assertTrue(name in wr.curr_exec['task_info'])
            self.assertTrue('start_time' in wr.curr_exec['task_info'][name])
            self.assertTrue(isinstance(wr.curr_exec['task_info'][name]['start_time'], float))

    def test_end_exec_task(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        name = 'myexec'
        for ekey, iw_exec in sorted(execs.items()):
            ow_exec = {'task_info': {}}
            wr.outputwcl[ekey] = ow_exec
            wr.curr_exec = ow_exec
            self.assertFalse(name in wr.curr_exec['task_info'])
            wr.start_exec_task(name)
            self.assertFalse('status' in wr.curr_exec['task_info'][name])
            time.sleep(0.2)
            wr.end_exec_task(2)
            self.assertTrue('status' in wr.curr_exec['task_info'][name])
            self.assertEqual(wr.curr_exec['task_info'][name]['status'], 2)
            self.assertTrue(wr.curr_exec['task_info'][name]['walltime'] >= 0.2)

    def test_end_all_tasks(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        names = ['myexec','anotherexec']
        for ekey, iw_exec in sorted(execs.items()):
            ow_exec = {'task_info': {}}
            wr.outputwcl[ekey] = ow_exec
            wr.curr_exec = ow_exec
            self.assertFalse(names[0] in wr.curr_exec['task_info'])
            wr.start_exec_task(names[0])
            time.sleep(0.2)

            self.assertFalse(names[1] in wr.curr_exec['task_info'])
            wr.start_exec_task(names[1])
            self.assertTrue(names[1] in wr.curr_exec['task_info'])
            self.assertTrue(names[0] in wr.curr_exec['task_info'])
            time.sleep(0.2)

            wr.end_all_tasks(18)
            for i, n in enumerate(names):
                self.assertTrue('status' in wr.curr_exec['task_info'][n])
                self.assertEqual(wr.curr_exec['task_info'][n]['status'], 18)
                self.assertTrue(wr.curr_exec['task_info'][n]['walltime'] >= 0.2 * i)

    def test_transform_inputs(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        names = ['myexec','anotherexec']
        for ekey, iw_exec in sorted(execs.items()):
            ow_exec = {'task_info': {}}
            wr.outputwcl[ekey] = ow_exec
            wr.curr_exec = ow_exec
            self.assertFalse('transform_inputs' in wr.curr_exec['task_info'])
            wr.transform_inputs(None)
            self.assertTrue('transform_inputs' in wr.curr_exec['task_info'])
            self.assertEqual(wr.curr_exec['task_info']['transform_inputs']['status'], 0)

    def test_check_inputs(self):
        touch_dir = 'mangle_tiles'
        touchfile = 'mangle_tiles/Y3A1v1_tolys_10s.122497.pol'
        try:
            os.unlink(touchfile)
        except:
            pass
        try:
            os.rmdir(touch_dir)
        except:
            pass
        try:
            wr = bwr.BasicWrapper(self.wcl_file)
            wr.outputwcl['wrapper']['start_time'] = time.time()
            execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
            ekey = list(execs.keys())[0]
            ow_exec = {'task_info': {}}
            wr.outputwcl[ekey] = ow_exec
            wr.curr_exec = ow_exec
            self.assertFalse('check_inputs' in wr.curr_exec['task_info'])
            with capture_output() as (out, _):
                self.assertRaises(SystemExit, wr.check_inputs, ekey)
                output = out.getvalue().strip()
                self.assertTrue('does not exist' in output)
            os.mkdir(touch_dir)
            open(touchfile, 'w').write("\n")

        finally:
            try:
                os.unlink(touchfile)
            except:
                pass
            try:
                os.rmdir(touch_dir)
            except:
                pass

    def test_check_command_line(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        ekey = list(execs.keys())[0]
        iw_exec = list(execs.values())[0]
        ow_exec = {'task_info': {}}
        wr.outputwcl[ekey] = ow_exec
        wr.curr_exec = ow_exec
        wr.check_command_line(ekey, iw_exec)
        self.assertEqual(ow_exec['task_info']['check_command_line']['status'], 0)

    def test_save_exec_version_none(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        ekey = list(execs.keys())[0]
        iw_exec = list(execs.values())[0]
        ow_exec = {'task_info': {}}
        wr.outputwcl[ekey] = ow_exec
        wr.curr_exec = ow_exec
        with capture_output() as (out, _):
            wr.save_exec_version(iw_exec)
            output = out.getvalue().strip()
            self.assertTrue('not find version' in output)

    def test_save_exec_version_partial(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        ekey = list(execs.keys())[0]
        iw_exec = list(execs.values())[0]
        ow_exec = {'task_info': {}}
        wr.outputwcl[ekey] = ow_exec
        wr.curr_exec = ow_exec
        iw_exec['execname'] = '/usr/bin/env echo'
        iw_exec['version_flag'] = '--version'
        with capture_output() as (out, _):
            wr.save_exec_version(iw_exec)
            output = out.getvalue().strip()
            self.assertTrue('not find version' in output)

    def test_save_exec_version_working(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        ekey = list(execs.keys())[0]
        iw_exec = list(execs.values())[0]
        ow_exec = {'task_info': {}}
        wr.outputwcl[ekey] = ow_exec
        wr.curr_exec = ow_exec
        iw_exec['execname'] = '/usr/bin/env echo'
        iw_exec['version_flag'] = '--version'
        iw_exec['version_pattern'] = 'echo\s+\(.*\)\s+(.*)'
        self.assertFalse('version' in wr.curr_exec)
        wr.save_exec_version(iw_exec)
        self.assertTrue('version' in wr.curr_exec)

    def test_save_exec_version_errors(self):
        wr = bwr.BasicWrapper(self.wcl_file)
        wr.outputwcl['wrapper']['start_time'] = time.time()
        execs = igm.get_exec_sections(wr.inputwcl, intgdefs.IW_EXEC_PREFIX)
        ekey = list(execs.keys())[0]
        iw_exec = list(execs.values())[0]
        ow_exec = {'task_info': {}}
        wr.outputwcl[ekey] = ow_exec
        wr.curr_exec = ow_exec
        iw_exec['execname'] = '/usr/bin/env echo'
        iw_exec['version_flag'] = '--version'
        iw_exec['version_pattern'] = 'echo\s+\(.*\)\s+(.*)'
        with patch('intgutils.basic_wrapper.subprocess.Popen', side_effect=OSError()):
            with capture_output() as (out, _):
                self.assertRaises(OSError, wr.save_exec_version, iw_exec)
                output = out.getvalue().strip()
                self.assertTrue('misspelled' in output)

        with patch('intgutils.basic_wrapper.subprocess.Popen', returncode=1):
            with capture_output() as (out, _):
                wr.save_exec_version(iw_exec)
                output = out.getvalue().strip()
                self.assertTrue('problem when running' in output)

        iw_exec['version_pattern'] = 'bad pattern'
        with capture_output() as (out, _):
            wr.save_exec_version(iw_exec)
            output = out.getvalue().strip()
            self.assertTrue('find version for' in output)


if __name__ == '__main__':
    unittest.main()
