#!/usr/bin/env python

""" Generic wrapper """

import sys
import argparse

import intgutils.basic_wrapper as basic_wrapper

def main():
    """ entry point """

    parser = argparse.ArgumentParser(description='Generic wrapper')
    parser.add_argument('inputwcl', nargs=1, action='store')
    args = parser.parse_args(sys.argv[1:])

    bwrap = basic_wrapper.BasicWrapper(args.inputwcl[0])
    bwrap.run_wrapper()
    bwrap.write_outputwcl()
    sys.exit(bwrap.get_status())

if __name__ == "__main__":
    main()
