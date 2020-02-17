import pandas as pd
import io
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine

class fastdnslog(object):
    def __init__(self):
        pass

    def read(self, filename):
        names=['subinfo', 'srcip', 'srcport', 'record', 'class', 'type', 'e0flag', 'e0size','dsecflag','tcpflag','res']

        self.df = pd.read_csv(filename, header=None, names=names)

        self.df['zoneid'] = self.df['subinfo'].apply(lambda x: int( x.split(' ')[0] ))
        self.df['utime'] = self.df['subinfo'].apply(lambda x: int( x.split(' ')[2] ))
        self.df['dtime'] = self.df['subinfo'].apply(self._parse_datetime)
        self.df = self.df.drop(['subinfo'],axis=1)

    def _parse_datetime(self, x):
        d, t = x.split(' ')[3:4+1]
        str_fmt = '{} {}'.format(d,t)
        return datetime.strptime(str_fmt, '%d/%m/%Y %H:%M:%S')


def do_test():
    f=fastdnslog()
    #f.read('samplelog.gz')
    f.read('head.txt')
    print(f.df.head())

if __name__ == '__main__':
    do_test()
