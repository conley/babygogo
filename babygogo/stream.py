#!/usr/bin/env python
# Author(s): 'Tommer Wizansky' <twizansk@gmail.com>
import abc
import uuid

import pylru


class Stream:
    """
    A Stream object represents an acyclic directed graph of processing nodes which handle and
    transform elements one at a time.  A single Stream is a linear chain of such nodes but multiple
    streams can be joined, and a stream can be split, to form complicated graphs.

    Nodes are added to a stream by using the mutation methods such as 'map', 'reduce' etc.
    """

    def __init__(self, stream_id=None):
        self.__head = None
        self.__filters = []
        self.id = stream_id if stream_id is not None else str(uuid.uuid1())

    def map(self, f):
        """
        Add a function to the tail of the stream, which accepts a single element and returns a
        transformed element

        :param callable f:  The processing function
        :return: self
        """
        self.append(Mapper(f))
        return self

    def reduce(self, f, initial):
        """
        Add a function to the tail of the stream, which accepts an element and accumulated value
        and returns an updated accumulated value.

        :param f:  The processing function
        :param initial:  The value to use to initialize the accumulated value.
        :return: self
        """
        self.append(Reducer(f, initial))
        return self

    def process(self, data):
        """
        process() is the entry point for the stream.  The supplied data represents a single element
        which will be passed to the nodes in the processing chain.

        :param object data:  The data element to be processed.
        """
        if self.__head:
            self.__head.send(Element(
                    stream_id=self.id,
                    data=data))

    def append(self, fltr):
        """Add a filter to a stream

        :param Filter fltr: The filter to add
        """
        it = fltr()
        next(it)
        if self.__head is None:
            self.__head = it
        else:
            self.__filters[-1].next = it
        self.__filters.append(fltr)


class Element:
    """
    An Element object wraps a single data element and adds metadata used by the stream framework
    """

    def __init__(self, stream_id, data):
        self.stream_id = stream_id
        self.data = data

    def __repr__(self):
        return "Element(stream_id={stream_id}, data={data})".format(**self.__dict__)


class Filter(metaclass=abc.ABCMeta):
    """
    Filter is a base class for all stream processing nodes. Filters are callable where the
    __call__ method processes the incoming data and relays the result to the downstream filters.
    """

    def __init__(self):
        self.next = None

    def send_next(self, element):
        if self.next is not None:
            self.next.send(element)

    @abc.abstractmethod
    def process(self, element):
        pass

    def __call__(self):
        while True:
            element = (yield)
            result = self.process(element)
            if result is not None:
                self.send_next(result)


class Mapper(Filter):
    """A type of filter which transforms a single data element"""

    def __init__(self, f):
        super().__init__()
        self.__f = f

    def process(self, element):
        result = self.__f(element.data)
        return Element(
                stream_id=element.stream_id,
                data=result)

    def copy(self):
        return Mapper(self.__f)


class Reducer(Filter):
    """
    Reducer is A type of Filter which implements a 'reduce' pattern, accumulating a value as
    elements of the stream are processed.
    """

    def __init__(self, f, initial):
        super().__init__()
        self.__f = f
        self.__acc = initial
        self.__initial = initial

    def process(self, element):
        self.__acc = self.__f(element.data, self.__acc)
        return Element(
                stream_id=element.stream_id,
                data=self.__acc)


class Joiner(Filter):
    """
    Joiner is a type of Filter which combines two streams (a 'left' and a 'right' stream) into one
    by joining on a key.   The incoming data elements are assumed to be size-two tuples where the
    first element is the key and the second is the value.

    Since the two incoming streams cannot be assumed to be synchronized, it is not possible to
    predict if a certain element from one stream will ever be joined by another.   To avoid
    accumulating data in memory indefinitely, a maximum size can be defined for the internal cache
    of the Joiner.  This is given by the 'max_size' constructor argument
    """

    def __init__(self, left, right, target, max_size=128):
        super().__init__()
        self.__cache_left = pylru.lrucache(size=max_size)
        self.__cache_right = pylru.lrucache(size=max_size)
        self.__left = left
        self.__right = right
        self.__target = target
        self.__max_size = max_size

    def process(self, element):
        """
        process() accepts an element and determines the source stream.  The cached values from the
        other stream are then searched for a match.  If one is found, the two elements are joined
        into one element of the form:

        (<key>, (<val left>, <val right>)

        If a match is not found, the element is cached for a possible future match.

        :param Element element:  The element to process.
        :return: The joined element or None if no match was found
        """
        stream_id = element.stream_id
        key = element.data[0]
        value = element.data[1]
        if stream_id == self.__left.id:
            cached = self.__cache_right.get(key)
            if cached is not None:
                return Element(
                        stream_id=self.__target.id,
                        data=(key, (value, cached)))
            else:
                self.__cache_left[key] = value
                return None
        else:
            cached = self.__cache_left.get(key)
            if cached is not None:
                return Element(
                        stream_id=self.__target.id,
                        data=(key, (cached, value)))
            else:
                self.__cache_right[key] = value
                return None


def join(left, right, max_size=128):
    """Join two streams into one.

    :param left: The left stream
    :param right: The right stream
    :param max_size: The maximum size of the Joiner's internal cache
    :return: The joined stream.
    """
    new_stream = Stream()
    joiner = Joiner(
            left=left,
            right=right,
            target=new_stream,
            max_size=max_size)
    left.append(joiner)
    right.append(joiner)
    new_stream.append(joiner)
    return new_stream
