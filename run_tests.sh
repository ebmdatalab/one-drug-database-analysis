#!/bin/bash

# This awkward testing of exit codes is to get around the case where
# no tests are found, which has exit code of 5 in pytest, but we don't
# want to treat as a failure

PYTHONPATH=$(pwd) python -m pytest --sanitize-with config/nbval_sanitize_file.conf --nbval notebooks; ret=$?; [ $ret = 5 ] && exit 0 || exit $ret
