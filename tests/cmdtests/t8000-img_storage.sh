#!/bin/bash
#
# Test dhcpd.conf file creation 
#
                                                                                                                                                                    
test_description='Test img_storage commands

General test to the img storage command lines
'

pushd `dirname $0` > /dev/null
export TEST_DIRECTORY=`pwd`
popd > /dev/null
. $TEST_DIRECTORY/test-lib.sh



if ! rpm -q rocks-command-kvm > /dev/null
then
	skip_all='KVM roll not installed'
	test_done
fi

if ! rpm -q rocks-command-imgstorage > /dev/null
then
	skip_all='img storage roll not installed'
	test_done
fi


test_expect_success 'test img_storage - steup' '
	rocks "add host" test-nas-0-0 cpus=4 rack=0 rank=0 membership=NAS\ Appliance os=linux &&
	rocks add host vm localhost compute name=test-vmhost 
'

test_expect_success 'test img_storage - set host vm nas' '
	test_must_fail rocks set host vm nas test-vmhost nas=naszzzzz &&
	test_must_fail rocks set host vm nas test-vmhost nas=test-nas-0-0 &&
	rocks set host vm nas test-vmhost nas=test-nas-0-0 zpool=zank321 &&
	rocks list host vm nas test-vmhost | grep zank321 &&
	echo testing attribute &&
	rocks set host attr test-nas-0-0 img_zpools zank123 &&
	rocks set host vm nas test-vmhost nas=test-nas-0-0 &&
	rocks list host vm nas test-vmhost | grep zank123 &&
	rocks set host attr test-nas-0-0 img_zpools value=zank123,zank111 &&
	rocks set host vm nas test-vmhost nas=test-nas-0-0
	rocks list host vm nas test-vmhost | grep " zank123\| zank111"
'


test_expect_success 'test img_storage - dump host vm nas' '
	rocks dump host vm nas | grep test-vmhost | \
		grep "=zank123\|=zank111"
'


test_expect_success 'test img_storage - teardown test' '
	rocks remove host test-nas-0-0 &&
	rocks remove host test-vmhost
	
'

test_done

