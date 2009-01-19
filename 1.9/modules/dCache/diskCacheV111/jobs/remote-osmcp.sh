#!/bin/sh
#
ssh oho-test "/usr/sbin/osmcp $*"
exit $?
