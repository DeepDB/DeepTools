#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Deep Information Sciences, Inc. - www.deep.is
#
# Released under the Apache 2.0 License
# 
# Author: Jason Jeffords, www.linkedin.com/in/JasonJeffords, @JasonJeffords
#
# This module requires mmh3, see: 
#	https://pypi.python.org/pypi/mmh3
#	https://github.com/hajimes/mmh3
#

import random, string, mmh3, struct, time

__S = None # transparently support multiprocessing through late initialization
def getSeed():
	global __S
	if not __S:
		__S = random.randint(0,2000000000) # create a large random seed
	return __S

def setSeed(integerSeed):
	global __S
	__S = integerSeed
	return __S

def incrementSeed():
	global __S
	__S = getSeed() + 1
	return __S

IPv4_PACKER = struct.Struct('i')  # pack 32 signed bits
def generateIPV4Address(): # pack 32 random bits in byte array
        return IPv4_PACKER.pack(setSeed(mmh3.hash('',getSeed()))) 

def generateIPV6Address(): # generate 128 random bits as byte array
        return mmh3.hash_bytes('', incrementSeed()) 

def generateString(size, fromCharacters=string.ascii_letters):
        return ''.join(random.choice(fromCharacters) for i in xrange(size))

def generateByteArray(size):
	# generate 128 random bits as byte array
        ba = mmh3.hash_bytes('', incrementSeed()) 
	for i in xrange(size/8):
		ba = ba + mmh3.hash_bytes('', incrementSeed())
	return bytearray(ba[0:size])
		

def choice(sequence):
	return sequence[randint32() % len(sequence)] 
def generateString2(size, fromCharacters=string.ascii_letters):
        return ''.join(random.choice(fromCharacters) for i in xrange(size))

def generateString3(size, fromCharacters=string.ascii_letters):
	ba = generateByteArray(size)
	lenChars = len(fromCharacters)
	chars = []
	for b in ba:
		chars.append(fromCharacters[b % lenChars])
	return ''.join(chars)

def randint32():
	return setSeed(mmh3.hash('',getSeed()))


def test():
	trials = 100000
	startTime = time.time()
	seed = mmh3.hash('s',42)
	for i in xrange(trials):
		generateString3(100)
		
	hashTime = time.time() - startTime
	print hashTime, trials/hashTime
	
	
	startTime = time.time()
	for i in xrange(trials):
		generateString(100)
	randTime = time.time() - startTime
	print randTime, trials/randTime

	print "randTime - hashTime = ", randTime-hashTime

if __name__ == '__main__':
	test()
