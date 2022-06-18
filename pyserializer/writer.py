# =================================================================
#
# Work of the U.S. Department of Defense, Defense Digital Service.
# Released as open source under the MIT License.  See LICENSE file.
#
# =================================================================

import gzip
import sys


class Writer(object):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __init__(self, writer=None, closer=None):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def write(self, data):
        raise NotImplementedError


class FileWriter(Writer):

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __init__(self, f):
        self.f = f

    def close(self):
        self.f.close()

    def write(self, data):
        if isinstance(data, str):
            self.f.write(data.encode())
        else:
            self.f.write(data)


class StreamWriter(Writer):

    def __init__(self, w):
        self.w = w

    def close(self):
        pass

    def write(self, data):
        if isinstance(data, str):
            self.w.write(data.encode())
        else:
            self.w.write(data)


def create_writer(compression=None, f=None):
    if compression == "gzip":
        if f == "-":
            return FileWriter(gzip.open(sys.stdout, 'wb'))
        return FileWriter(gzip.open(f, 'wb'))
    elif compression is None or len(compression) == 0:
        if isinstance(f, str):
            if f == "-":
                return StreamWriter(sys.stdout)
            return FileWriter(open(f, 'wb'))
        else:
            return FileWriter(f)
    else:
        raise Exception("unknown compression {}".format(compression))
