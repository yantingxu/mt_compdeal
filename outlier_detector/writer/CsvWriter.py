#!/usr/local/bin/python
#coding=utf-8
import sys
import csv
reload(sys)
sys.setdefaultencoding('utf-8')
from Writer import Writer


class CsvWriter(Writer) :
    """Mysql 数据写回"""

    def __init__(self) :
        super(Writer, self).__init__()

    def before_write(self) :
        pass

    def after_write(self) :
        pass

    def write(self, data, outfile, header, isdict = True) :
        self.before_write()
        self.write_raw(data, outfile, header, isdict)
        self.after_write()

    def write_raw(self, data, outfile, header, isdict = True) :
        foutput = csv.writer(file(outfile, 'w'), dialect = csv.excel)
        foutput.writerow(header)
        for item in data:
            if isdict:
                row = []
                for key in header:
                    row.append(item[key])
                foutput.writerow(row)
            else:
                foutput.writerow(item)
