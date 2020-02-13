#!/usr/bin/env python3

import unittest
import os
import sys
from contextlib import contextmanager
from io import StringIO
from mock import patch

import intgutils.intgmisc as igm
import intgutils.replace_funcs as rf
import intgutils.wcl as wcl

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

    def test_search(self):
        w = wcl.WCL()
        with open(self.wcl_file, 'r') as infh:
            w.read(infh, self.wcl_file)
        res = w.search('directory_pattern.inputwcl')

        self.assertTrue(res[0])
        self.assertEqual(res[1]['name'], 'wcl')

        res2 = w.search('dirn')
        self.assertFalse(res2[0])

        res = w.search('filename_pattern')

        res2 = w.search('filename_pattern', {'currentvals': {'runjob': 'rnj.sh'}})
        print(res2[1]['runjob'])
        self.assertTrue(res2[0])
        self.assertEqual(res2[1]['runjob'], 'runjob.sh')
        self.assertEqual(res, res2)

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

if __name__ == '__main__':
    unittest.main()
