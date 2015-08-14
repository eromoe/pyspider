#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: eromoe
# Created on 2015-08-14 22:23:10

import logging
import pybloom
import os
import six

try:
	import pyreBloom
except ImportError:
	pass


logger = logging.getLogger('filter')


###  pyreBloom

# >>> p.add('xxxx')
# 1
# >>> p.add('xxxx')
# 0
# >>> p.add('xxxx')
# 0

# >>> p.contains('xxx')
# True
# >>> p.contains(['xxx'])
# ['xxx']
# >>> p.contains(['dsds'])
# []

############################

###  pybloom

# >>> bf.add('rrrr')
# False
# >>> bf.add('rrrr')
# True
# >>> bf.add('rrrr')
# True


class BaseFilter(object):
	def add(self, value):
		raise NotImplementedError

	def extend(self, values):
		raise NotImplementedError

	def contains(self, values):
		raise NotImplementedError

	def tofile(self, key):
		pass

	def fromfile(self, key):
		pass


class RedisBloomFilter(BaseFilter):
	def __init__(self, key, capacity=10000, error_rate=0.001, host='127.0.0.1', port=6379, password='', db=0):
		self.bf = pyreBloom.BloomFilter(key, capacity=capacity, error_rate=error_rate)

	def add(self, value):
		return not bool(self.bf.add(value))

	def extend(self, values):
		return self.bf.extend(values)

	def contains(self, values):
		return self.bf.contains(values)



class BloomFilter(BaseFilter):
	def __init__(self, key, capacity=10000, error_rate=0.001, store_dir='~/pybloom'):
		if not os.path.exists(store_dir):
			os.makedirs(store_dir)

		self.key = key
		self.path = os.path.join(store_dir, key)

		if os.path.exists(self.path):
			logger.warn('BloomFilter path:%s  already exists, the file would be overwrite when call tofile , be careful !' % self.path)

		self.bf = pybloom.BloomFilter(capacity=capacity, error_rate=error_rate)


	def add(self, value):
		return self.bf.add(value)

	def extend(self, values):
		if hasattr(values, '__iter__'):
			return [self.bf.add(v) for v in value]
		else:
			return [self.bf.add(v)]

	def contains(self, values):
		# follow pyreBloom's contains behavious
		if hasattr(values, '__iter__'):
			return [v in self.bf for v in value]
		else:
			return v in self.bf

	def tofile(self):
		with open(self.path, 'wb') as f:
			self.bf.tofile(f)

	def fromfile(self):
		with open(self.path, 'rb') as f:
			self.bf.fromfile(f)