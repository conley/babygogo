#!/usr/bin/env python
# Author(s): 'Tommer Wizansky' <twizansk@gmail.com>
import abc
import logging
from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class Source(metaclass=abc.ABCMeta):
    """Base class for Babygogo source classes.

    The concrete source classes are responsible for retreiving incoming stream elements from
    external systems and passing them to Stream instances for processing.
    """

    def __init__(self):
        self._stream = None

    def attach(self, stream):
        """
        Attach a stream to the source.  Once the source's 'start' method is called, the incoming
        elements will be passed to the stream for processing, one at a time.

        :type stream: babygogo.stream.Stream
        """
        self._stream = stream

    @abc.abstractmethod
    def start(self):
        pass

    def _process(self, element):
        if self._stream is not None:
            self._stream.process(element)
        else:
            logger.warn('Received element with no attached stream.  Discarding.')


class IterableSource(Source):

    def __init__(self, itr):
        super().__init__()
        self.__itr = itr

    def start(self):
        for e in self.__itr:
            self._stream.process(e)


class KafkaSource(Source):
    """A simple Kafka source for Babygogo streams

    KafkaSource reads messages from a Kafka topic and passes the value of each message to an
    attached stream for processing.

    Keyword Arguments:
        hosts:  list of host and port pairs in the format <host>:<port>
        topic: topic to which the source will listen
    """

    def __init__(self, hosts, topic):
        super().__init__()
        self.__topic = topic
        self.__hosts = hosts

    def start(self):
        consumer = KafkaConsumer(self.__topic, bootstrap_servers=self.__hosts)
        for record in consumer:
            self._process(record.value)
