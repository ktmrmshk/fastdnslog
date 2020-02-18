import pandas as pd
import io
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine


class logkit(object):
    def __init__(self):
        self.df=None

    def read(self, filenmae):
        'should be implemented at child class'
        pass

    def init_engine(self, dbhost='localhost', dbport=3306, dbuser='root', dbpasswd='passwd', dbname='fastdnslog'):
        url = 'mysql+mysqlconnector://{}:{}@{}:{}'.format(dbuser, dbpasswd, dbhost, dbport)
        self.eng = create_engine(url)
        self.eng.execute("CREATE DATABASE  IF NOT EXISTS {}".format(dbname))
        self.eng.execute("USE {}".format(dbname))

    def push(self, tablename, if_exists='append'):
        self.df.to_sql(tablename, con=self.eng, if_exists=if_exists, chunksize=1000)




class fastdnslog(logkit):
    def __init__(self):
        super().__init__()

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


class zonefiler(logkit):
    def __init__(self):
        super().__init__()

    def read(self, filename):
        names = ['record', 'ttl', 'class', 'type', 'val']
        self.df = pd.read_csv(filename, header=None, comment=';', sep='\t', names=names)
        self.df['record'] = self.df['record'].apply(lambda x: x[:-1])
    
def do_test1():
    f=fastdnslog()
    #f.read('samplelog.gz')
    f.read('head.txt')
    print(f.df.head())
    
    f.init_engine()
    f.push('foobar')

def do_test():
    z=zonefiler()
    z.read('example.zone')
    z.init_engine()
    z.push('zone_sgn_suntory_co_jp')




if __name__ == '__main__':
    do_test()
