#!/usr/bin/env python2

import unittest
import filecmp
import os

import intgutils.intgmisc as igm
import intgutils.wcl as wcl

ROOT = '/var/lib/jenkins/test_data/'

class TestIntgmisc(unittest.TestCase):
    def test_check_files(self):
        files = [ROOT + 'raw/test_raw.fits.fz', ROOT + 'raw/notthere.fits']
        (exist, missing) = igm.check_files(files)
        self.assertEqual(len(exist), 1)
        self.assertEqual(len(missing), 1)
        self.assertTrue('test_raw.fits' in exist[0])
        self.assertTrue('notthere' in missing[0])

class TestWCL(unittest.TestCase):
    def test_init(self):
        try:
            os.unlink('out.wcl')
        except:
            pass
        fl = ROOT + 'wcl/TEST_DATA_r15p03_full_config.des'
        w = wcl.WCL()
        with open(fl, 'r') as infh:
            w.read(infh, fl)
        self.assertEqual(w['attnum'], str(3))
        w.write(open('out.wcl', 'w'))
        self.assertTrue(filecmp.cmp(fl, 'out.wcl'))

if __name__ == '__main__':
    unittest.main()
