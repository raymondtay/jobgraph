#!/bin/env python2
#
# Change to 'env python3' if there's a need to upgrade
#

import os

def test() :
  return os.name

if __name__ == "__main__":
    print 'Invoked by DataflowRunner. Operating System is: {}'.format(test())
