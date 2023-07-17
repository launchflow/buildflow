# distutils: language=c++
# distutils: sources = pubsub_stream.cpp

from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "buildflow/core/io/gcp/strategies/_cython/pubsub_stream.h" namespace "buildflow":
    cdef cppclass CPubSubData:
        string data
        string ack_id

        CPubSubData(string data, string ack_id) except +

    cdef cppclass CPubSubStream:
        string subscription_id

        CPubSubStream(string) except +
        vector[CPubSubData] pull()
        void ack(vector[string])


cdef class PyPubSubData:
    cdef CPubSubData *thisptr
    def __cinit__(self, string data, string ack_id):
        self.thisptr = new CPubSubData(data, ack_id)

    def __dealloc__(self):
        del self.thisptr

    def ack_id(self):
        return self.thisptr.ack_id

    def data(self):
        return self.thisptr.data

cdef class PyPubSubStream:
    cdef CPubSubStream *thisptr
    def __cinit__(self, string subscription_id):
        self.thisptr = new CPubSubStream(subscription_id)
    def __dealloc__(self):
        del self.thisptr
    def pull(self):
        cdata = self.thisptr.pull()
        out_list = []
        for i in range(cdata.size()):
            out_list.append(PyPubSubData(cdata[i].data, cdata[i].ack_id))
        return out_list
    def ack(self, vector[string] ack_ids):
        return self.thisptr.ack(ack_ids)
