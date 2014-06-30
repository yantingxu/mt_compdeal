#!/usr/local/bin/python
#coding=utf-8
import sys
import csv
reload(sys)
sys.setdefaultencoding('utf-8')
from Reader import Reader

class CsvReader(Reader) :
    """Csv 获取数据"""

    def __init__(self, **meta) :
        super(Reader, self).__init__()

    def before_read(self) :
        pass

    def after_read(self) :
        pass

    def read_raw(self, infile, return_dict = True):
        finput = csv.reader(file(infile))
        header = finput.next()
        result = []
        for row in finput:
            if return_dict:
                out_dict = {}
                for index, key in enumerate(header):
                    out_dict[key] = row[index]
                result.append(out_dict)
            else:
                result.append(row) 

        return result

    def read(self, infile, return_dict = True ) :
        self.before_read()
        result = self.read_raw(infile, return_dict)
        self.after_read()
        return result

