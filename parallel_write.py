#!/bin/python

import threading
import random
import sys
import time
import os
import subprocess
import string
import xattr

max_depth=10

class mythread (threading.Thread):
	def __init__(self, threadID, path, size, op, nprocs):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.path = path
		self.size = size
		self.op = op
		self.nprocs = nprocs
	
	def run(self):
		print self.threadID,self.path,self.size
		if op:
			self.do_write()

	def do_write(self):
		fd = os.open(self.path, os.O_RDWR)
		chunk_size = self.size/self.nprocs
		my_offset = chunk_size * self.threadID 
		buf = "a"*chunk_size
		attr_name = str(self.threadID)
		attr_value = "True"
	
		os.lseek(fd, my_offset, 0)
		ret = os.write(fd, buf)
		if (ret == chunk_size):
			print "success write"
			print self.path
		else:
			print "write failed"
		xattr.setxattr (self.path, "user."+attr_name, attr_value)
		os.close(fd)
	
			

def gen_rand_path():
	depth = random.randrange(2,max_depth)
	rand_path=""			
	
	for i in range(depth):
		rand_path = os.path.join(rand_path, rand_gen(random.randrange(10)))

	print "randpath=", rand_path
	return rand_path
	


def rand_gen(size=6, chars=string.ascii_lowercase + string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for _ in range(size))
	

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print "Usage:- %s <nrpocs> <timeout> <keyring> <user>" %sys.argv[0]
		exit()

	nprocs = int(sys.argv[1])
	timeout = int(sys.argv[2])
	keyring = sys.argv[3]
	user = sys.argv[4]
	start = current = time.time()
	fuse_mnt = []
	kmnt = []
	kroot = "/mnt/kernel"
	froot = "/mnt/ceph-fuse"
	kmnt_cmd_prefix = "mount -t ceph"
	fuse_cmd_prefix = "ceph-fuse  -m "
	monitor = "magna070"

	#create dirs for kmount and fuse mount
	if not os.path.exists(kroot):
		os.makedirs(kroot)

	if not os.path.exists(froot):
		os.makedirs(froot)

	# number of mounts = number of thREads
	kmounts = (lambda:(nprocs/2)+1, lambda:(nprocs/2)) [nprocs%2 == 0]()
	fmounts = nprocs/2

	for i in range(kmounts):
		kmnt.append(os.path.join(kroot, "d"+str(i)))

	for i in range(fmounts):
		fuse_mnt.append(os.path.join(froot, "d"+str(i)))

	#Do actual mount on respective mounts points
	# kernel mounts first then fuse	
	for i in kmnt:
		if not os.path.exists(i):
			os.makedirs(i)
		cmd = kmnt_cmd_prefix + " " + monitor + ":/ " + i + " -o "\
			+ "name=" + user + ",secret=" + keyring 
		print cmd
		p = subprocess.Popen("umount "+i, shell=True, stdout=subprocess.PIPE,\
					stderr=subprocess.STDOUT)
		for line in p.stdout.readlines():
			print line
		p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, \
					stderr=subprocess.STDOUT)
		for line in p.stdout.readlines():
			print line
	for i in fuse_mnt:
		if not os.path.exists(i):
			os.makedirs(i)
		
		cmd = fuse_cmd_prefix + monitor + ":6789 " + i
		print cmd
		p = subprocess.Popen("umount "+i, shell=True, stdout=subprocess.PIPE,\
					stderr=subprocess.STDOUT)
		for line in p.stdout.readlines():
			print line
		p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, \
					stderr=subprocess.STDOUT)
		for line in p.stdout.readlines():
			print line
	
	while (timeout > int(current-start)):	
		path = gen_rand_path()
		abs_path=[]
		for i in range(kmounts):
			abs_path.append(os.path.join(kmnt[i],path))
		for i in range(fmounts):
			abs_path.append(os.path.join(fuse_mnt[i],path))

		print "total = " ,abs_path
			
		
		#fpath=[]
	#	fpath = os.path.join(abs_path,filename)
			
		op=1 #only write as of now
		filename = rand_gen()
		filename = filename + ".txt"
		for i in range(kmounts+fmounts):
			if not os.path.exists(abs_path[i]):
				os.makedirs(abs_path[i])
			abs_path[i] = os.path.join(abs_path[i], filename)
			

		fd = os.open(abs_path[0], os.O_RDWR|os.O_CREAT)
		
		s_unit = [1024, 1048576]	
		f_size = random.randint(1,5) * s_unit[random.randint(0,len(s_unit)-1)] 
		os.ftruncate(fd, f_size)	
		os.close(fd)

		t_array=[]	
		#spawn threads with appropriate params
		for i in range(nprocs):
			t_array.append(mythread(i, abs_path[i], f_size, op, nprocs))
				
		for i in range(nprocs):
			t_array[i].start()	

		current = time.time()
