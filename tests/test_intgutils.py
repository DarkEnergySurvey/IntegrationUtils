#!/usr/bin/env python3

import unittest
import os
import sys
from contextlib import contextmanager
from io import StringIO

import intgutils.intgmisc as igm
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
    wcl_file = ROOT + 'wcl/TEST_DATA_r15p03_full_config.des'

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
        self.assertTrue(procinfo['ru_stime'] > 0.)


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

        #res = w.search('filename_pattern')

        #res2 = w.search('filename_pattern', {'currentvals': {'runjob': 'rnj.sh'}})
        #print(res2[1]['runjob'])
        #self.assertTrue(res2[0])
        #self.assertEqual(res2[1]['runjob'], 'rnj.sh')
        #self.assertNotEqual(res, res2)

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
