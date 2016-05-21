import pycurl
import traceback
import time
import os
from urllib.parse import urlencode
from fcntl import LOCK_EX, LOCK_SH, LOCK_NB, LOCK_UN, flock, lockf

from fsync.common.log import logger
from fsync.conf import SynConfig

class SynCurl:
    Normal = 0
    Upload = 1
    Download = 2

    def __init__(self):
        self.__response = ''
        self.__op = None
        self.__fd = None
        self.__startpos = 0
        self.__endpos = None
        self.__buffer = ''

    @staticmethod
    def __init_cipher(crypt, key):
        if crypt == '1':
            return ARC4.new(key)
        elif crypt == '2':
            return Blowfish.new(key, Blowfish.MODE_CFB, segment_size=8)
        elif crypt == '3':
            return AES.new(key.ljust(32, '.')[0:32], AES.MODE_CFB, segment_size=8)
        else:
            return None

    def __write_data(self, rsp):
        if self.__op == SynCurl.Download:
            if self.__startpos + len(self.__buffer) + len(rsp) - 1 > self.__endpos:
                return 0
            if SynConfig.config['encryption'] == '0':
                self.__fd.write(rsp)
                self.__startpos += len(rsp)
            else:
                self.__buffer += rsp
                while len(self.__buffer) >= 4096 or self.__startpos + len(self.__buffer) - 1 == self.__endpos:
                    cipher = self.__init_cipher(SynConfig.config['encryption'], SynConfig.config['encryptkey'])
                    self.__fd.write(cipher.decrypt(self.__buffer[0:4096]))
                    self.__startpos += 4096
                    self.__buffer = self.__buffer[4096:]
        else:
            self.__response += rsp.decode('utf-8')
        return len(rsp)

    def __read_data(self, size):
        if self.__startpos > self.__endpos:
            return ''
        elif self.__startpos + size - 1 > self.__endpos:
            size = self.__endpos - self.__startpos + 1

        if SynConfig.config['encryption'] == '0':
            self.__startpos += size
            return self.__fd.read(size)
        else:
            while len(self.__buffer) < size:
                rst = self.__fd.read(4096)
                if rst:
                    cipher = self.__init_cipher(SynConfig.config['encryption'], SynConfig.config['encryptkey'])
                    self.__buffer += cipher.encrypt(rst)
                else:
                    break
            rst = self.__buffer[0:size]
            self.__buffer = self.__buffer[size:]
            self.__startpos += size
            return rst

    @staticmethod
    def __write_header(rsp):
        return len(rsp)

    def request(self, url, querydata, rdata, method='POST', rtype=0, fnname=''):
        retrycnt = -1
        self.__op = rtype
        if querydata:
            if 'path' in querydata:
                querydata['path'] = querydata['path'].encode('utf-8')
        while retrycnt < SynConfig.config['retrytimes']:
            retrycnt += 1
            logger.debug('Start curl request(%s) %d times for %s.' % (rdata, retrycnt, fnname))
            if self.__op != SynCurl.Normal:
                startpos, self.__endpos = rdata.split('-', 1)
                startpos = self.__startpos = int(startpos)
                self.__endpos = int(self.__endpos)
            self.__response = ''
            curl = pycurl.Curl()
            try:
                if querydata:
                    url += '?%s' % urlencode(querydata)
                curl.setopt(pycurl.URL, url)
                curl.setopt(pycurl.SSL_VERIFYPEER, 0)
                curl.setopt(pycurl.SSL_VERIFYHOST, 0)
                curl.setopt(pycurl.FOLLOWLOCATION, 1)
                curl.setopt(pycurl.CONNECTTIMEOUT, 15)
                curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)
                curl.setopt(pycurl.LOW_SPEED_TIME, 30)
                curl.setopt(pycurl.USERAGENT, '')
                curl.setopt(pycurl.HEADER, 0)
                curl.setopt(pycurl.NOSIGNAL, 1)
                curl.setopt(pycurl.WRITEFUNCTION, self.__write_data)

                starthour, endhour = SynConfig.config['speedlimitperiod'].split('-', 1)
                starthour = int(starthour)
                endhour = int(endhour)
                curhour = time.localtime().tm_hour
                if (endhour > starthour and starthour <= curhour < endhour) or (endhour < starthour and (curhour < starthour or curhour >= endhour)):
                    curl.setopt(pycurl.MAX_SEND_SPEED_LARGE, SynConfig.config['maxsendspeed'] / SynConfig.config['tasknumber'] / SynConfig.config['threadnumber'])
                    curl.setopt(pycurl.MAX_RECV_SPEED_LARGE, SynConfig.config['maxrecvspeed'] / SynConfig.config['tasknumber'] / SynConfig.config['threadnumber'])
                if self.__op == SynCurl.Upload:
                    curl.setopt(pycurl.UPLOAD, 1)
                    curl.setopt(pycurl.READFUNCTION, self.__read_data)
                    curl.setopt(pycurl.INFILESIZE, self.__endpos - startpos + 1)
                    with open(fnname, 'rb') as self.__fd:
                        self.__fd.seek(startpos)
                        flock(self.__fd, LOCK_SH)
                        curl.perform()
                        flock(self.__fd, LOCK_UN)
                elif self.__op == SynCurl.Download:
                    curl.setopt(pycurl.RANGE, rdata)
                    with open(fnname, 'rb+') as self.__fd:
                        self.__fd.seek(startpos)
                        lockf(self.__fd, LOCK_EX, self.__endpos - startpos + 1, startpos, 0)
                        curl.perform()
                        self.__fd.flush()
                        os.fsync(self.__fd.fileno())
                        lockf(self.__fd, LOCK_UN, self.__endpos - startpos + 1, startpos, 0)
                else:
                    curl.setopt(pycurl.CUSTOMREQUEST, method)
                    if method == 'POST':
                        curl.setopt(pycurl.POSTFIELDS, urlencode(rdata))
                    curl.perform()
                retcode = curl.getinfo(pycurl.HTTP_CODE)
                if retcode < 400 or retcode == 404 or retrycnt == SynConfig.config['retrytimes']:
                    if retcode != 200 and retcode != 206 and self.__response == '':
                        self.__response = '{"error_code":%d,"error_msg":"Returned by the server is not in the expected results."}' % retcode
                    return retcode, self.__response
                else:
                    time.sleep(SynConfig.config['retrydelay'])
            except pycurl.error as error:
                if retrycnt == SynConfig.config['retrytimes']:
                    (errno, errstr) = error
                    return errno, '{"error_code":%d,"error_msg":"%s"}' % (errno, errstr)
            except Exception as e:
                return -1, '{"error_code":%d,"error_msg":"%s"}' % (-1, traceback.format_exc().replace('\n', '\\n').replace('"', '\''))
            finally:
                curl.close()
                logger.debug('Complete curl request(%s) %d times for %s.' % (rdata, retrycnt, fnname))
