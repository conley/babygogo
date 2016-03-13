#!/usr/bin/env python
# Author(s): 'Tommer Wizansky' <twizansk@gmail.com>

import unittest

from babygogo.stream import Stream, join


def times_2(element):
    return element * 2


def add(element, acc):
    return element + acc


class Cache:

    def __init__(self):
        self.cached = []

    def __call__(self, element):
        self.cached.append(element)


class TestStream(unittest.TestCase):

    def test_map(self):
        stream = Stream()
        cache = Cache()
        stream = stream\
            .map(times_2)\
            .map(cache)
        stream.process(4)
        print(cache.cached)
        self.assertEquals(cache.cached, [8])

    def test_reduce(self):
        stream = Stream()
        cache = Cache()
        stream = stream\
            .reduce(add, 1)\
            .map(cache)
        stream.process(4)
        stream.process(4)
        stream.process(4)
        print(cache.cached)
        self.assertEquals(cache.cached, [5, 9, 13])

    def test_join(self):
        cache = Cache()
        stream1 = Stream()
        stream2 = Stream()
        join(stream1, stream2)\
            .map(cache)
        stream1.process((1, 1))
        stream2.process((1, 2))
        stream1.process((2, 3))
        stream2.process((2, 5))
        print(cache.cached)
        self.assertEquals(cache.cached, [(1, (1, 2)), (2, (3, 5))])



