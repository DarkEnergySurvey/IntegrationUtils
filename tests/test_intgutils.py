#!/usr/bin/env python2

import unittest

import intgutils.intgmisc as igm

ROOT = '/var/lib/jenkins/test_data/'

class TestIntgmisc(unittest.TestCase):
    def test_check_files(self):
        files = [ROOT + 'raw/test_raw.fits.fz', ROOT + 'raw/notthere.fits']
        (exist, missing) = igm.check_files(files)
        self.assertEqual(len(exist), 1)
        self.assertEqual(len(missing), 1)
        self.assertTrue('test_raw.fits' in exist[0])
        self.assertTrue('notthere' in missing[0])


if __name__ == '__main__':
    unittest.main()
