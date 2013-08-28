#!/usr/bin/python

import os
import argparse

parser = argparse.ArgumentParser(description='zfs automatic incremental send receive')
parser.add_argument('source', help='source zpool')
parser.add_argument('destination', help='destination zpool')

opts = vars(parser.parse_args())
source=opts['source']
destination=opts['destination']
print("Source zpool: "+ source)
print("Destination zpool: "+ destination)

datasets=os.popen("zfs list -o name | grep "+destination).read().strip().split()
del datasets[0]

for dataset in datasets:                                                                                                                        
     	dataset=dataset.lstrip(destination).lstrip("/")
     	print "Now send | receiving dataset " + dataset                                                                                 
     	last=os.popen("zfs list -t snapshot -o name | grep "+destination+"/"+dataset+"@zfs-auto-snap_monthly | tail -1").read()                                                   
     	last=last.partition('@')[2].strip()                                                                                                                 
     	latest = os.popen("zfs list -t snapshot -o name | grep "+source+"/"+dataset+"@zfs-auto-snap_monthly | tail -1").read()
	latest=latest.partition('@')[2].strip()
	command="zfs send -i "+source+"/"+dataset+"@"+last+" "+source+"/"+dataset+"@"+latest+" | zfs recv "+destination+"/"+dataset                 
     	print command

