import fastdnslog as flog
import sys, logging

def usage():
    print('usage: python zonefiler_app zone_file_name    tablename')
    print('ex   : python zonefiler_app exmaple.com.zone  zone_example')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) != 3:
        usage()
        exit()

    filename=sys.argv[1]
    tablename=sys.argv[2]

    logging.debug('filename => {}'.format(filename) )
    logging.debug('tablename => {}'.format(tablename) )


    z=flog.zonefiler()
    z.read(filename)
    z.init_engine(dbport=13306)
    z.push(tablename)


