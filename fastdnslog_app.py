import fastdnslog as flog
import sys, logging

def usage():
    print('usage: python fastdnslog_app log_file_name  tablename')
    print('ex   : python fastdnslog_app fastdns_log.gz blog     ')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 3:
        usage()
        exit()

    filename=sys.argv[1]
    tablename=sys.argv[2]

    logging.debug('filename => {}'.format(filename) )
    logging.debug('tablename => {}'.format(tablename) )


    f=flog.fastdnslog()
    f.read(filename)
    f.init_engine(dbport=13306)
    f.push(tablename)


