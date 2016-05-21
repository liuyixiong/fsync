import json
import os

from fsync.common.log import logger
from fsync.conf import SynConfig
from fsync.fcurl import SynCurl

class BaiduPcsApi:
    @staticmethod
    def get_pcs_quota():
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/quota'
        querydata = {
                'method': 'info',
                'access_token': SynConfig.token['access_token']
                }
        retcode, responses = sycurl.request(url, querydata, '', 'GET', SynCurl.Normal)
        responses = json.loads(responses)
        if retcode != 200 or 'error_code' in responses:
            logger.error('Errno:%d: Get pcs quota failed: %s.' % (retcode, responses['error_msg']))
            return 1
        logger.info(' PCS quota is %dG,used %dG.' % (responses['quota'] / 1024 / 1024 / 1024, responses['used'] / 1024 / 1024 / 1024))
        return 0

    @staticmethod
    def get_pcs_filelist(pcspath, startindex, endindex):
        logger.debug('Start get pcs file list(%d-%d) of "%s".' % (startindex, endindex, pcspath))
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method'      : 'list',
                'access_token': SynConfig.token['access_token'],
                'path'        : pcspath,
                'by'          : 'name',
                'order'       : 'asc',
                'limit'       : '%d-%d' % (startindex, endindex),
                }
        retcode, responses = sycurl.request(url, querydata, '', 'GET', SynCurl.Normal)
        try:
            responses = json.loads(responses)
            if retcode != 200 or 'error_code' in responses:
                if responses['error_code'] == 31066:
                    return 31066, []
                else:
                    logger.error('Errno:%d: Get PCS file list of "%s" failed: %s.' % (retcode, pcspath, responses['error_msg']))
                    return 1, []
            return 0, responses['list']
        except Exception as e:
            logger.error('Get PCS file list of "%s" failed. return code: %d, response body: %s.\n%s\n%s' % (pcspath, retcode, str(responses), e, traceback.format_exc()))
            return 1, []
        finally:
            del responses
            logger.debug('Complete get pcs file list(%d-%d) of "%s".' % (startindex, endindex, pcspath))

    @staticmethod
    def create_pcsdir(pcspath):
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method': 'mkdir',
                'access_token': SynConfig.token['access_token'],
                'path': pcspath
                }
        retcode, responses = sycurl.request(url, querydata, '', 'POST', SynCurl.Normal)
        responses = json.loads(responses)
        if retcode == 200 and responses['path'] == pcspath:
            return 0
        logger.error('Errno:%d: Create PCS directory "%s" failed: %s.' % (retcode, pcspath, responses['error_msg']))
        return 1

    @staticmethod
    def check_create_pcsdir(pcspath):
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method': 'meta',
                'access_token': SynConfig.token['access_token'],
                'path': pcspath
                }
        retcode, responses = sycurl.request(url, querydata, '', 'GET', SynCurl.Normal)
        try:
            responses = json.loads(responses)
            if retcode == 200 and responses['list'][0]['isdir'] == 1:
                return 0
            elif (retcode != 200 and responses['error_code'] == 31066) or (retcode == 200 and responses['list'][0]['isdir'] == 0):
                url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
                querydata = {
                        'method': 'mkdir',
                        'access_token': SynConfig.token['access_token'],
                        'path': pcspath
                        }
                retcode, responses = sycurl.request(url, querydata, '', 'POST', SynCurl.Normal)
                responses = json.loads(responses)
                if retcode == 200 and responses['path'] == pcspath:
                    return 0
            logger.error('Errno:%d: Create PCS directory "%s" failed: %s.' % (retcode, pcspath, responses['error_msg']))
            return 1
        except Exception as e:
            logger.error('Create PCS directory "%s" failed. return code: %d, response body: %s.\n%s\n%s' % (pcspath, retcode, str(responses), e, traceback.format_exc()))
            return 1

    @staticmethod
    def rm_pcsfile(pcspath, slient=False):
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method': 'delete',
                'access_token': SynConfig.token['access_token'],
                'path': pcspath
                }
        retcode, responses = sycurl.request(url, querydata, '', 'POST', SynCurl.Normal)
        responses = json.loads(responses)
        if retcode != 200 or 'error_code' in responses:
            if not slient:
                logger.error('Errno:%d: Delete remote file or directory "%s" failed: %s.' % (retcode, pcspath, responses['error_msg']))
            return 1
        if not slient:
            logger.info(' Delete remote file or directory "%s" completed.' % (pcspath))
        return 0

    @staticmethod
    def mv_pcsfile(oldpcspath, newpcspath, slient=False):
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method': 'move',
                'access_token': SynConfig.token['access_token'],
                'from': oldpcspath,
                'to': newpcspath
                }
        retcode, responses = sycurl.request(url, querydata, '', 'POST', SynCurl.Normal)
        responses = json.loads(responses)
        if retcode != 200 or 'error_code' in responses:
            if not slient:
                logger.error('Errno:%d: Move remote file or directory "%s" to "%s" failed: %s.' % (retcode, oldpcspath, newpcspath, responses['error_msg']))
            return 1
        if not slient:
            logger.info(' Move remote file or directory "%s" to "%s" completed.' % (oldpcspath, newpcspath))
        return 0

    @staticmethod
    def cp_pcsfile(srcpcspath, destpcspath):
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method': 'copy',
                'access_token': SynConfig.token['access_token'],
                'from': srcpcspath,
                'to': destpcspath
                }
        retcode, responses = sycurl.request(url, querydata, '', 'POST', SynCurl.Normal)
        responses = json.loads(responses)
        if retcode != 200 or 'error_code' in responses:
            logger.error('Errno:%d: Copy remote file or directory "%s" to "%s" failed: %s.' % (retcode, srcpcspath, destpcspath, responses['error_msg']))
            return 1
        logger.info(' Copy remote file or directory "%s" to "%s" completed.' % (srcpcspath, destpcspath))
        return 0

    @staticmethod
    def get_pcs_filemeta(pcspath):
        sycurl = SynCurl()
        url = 'https://pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method': 'meta',
                'access_token': SynConfig.token['access_token'],
                'path': pcspath
                }
        retcode, responses = sycurl.request(url, querydata, '', 'GET', SynCurl.Normal)
        responses = json.loads(responses)
        if retcode != 200 or 'error_code' in responses:
            logger.error('Errno:%d: Get file\'s meta failed: %s, %s.' % (retcode, pcspath, responses['error_msg']))
            return 1, {}
        return 0, responses['list'][0]

    @staticmethod
    def upload_file_nosync(filepath, pcspath):
        sycurl = SynCurl()
        url = 'https://c.pcs.baidu.com/rest/2.0/pcs/file'
        querydata = {
                'method': 'upload',
                'access_token': SynConfig.token['access_token'],
                'path': pcspath,
                'ondup': 'newcopy'
                }
        retcode, responses = sycurl.request(url, querydata, '0-%d' % (os.stat(filepath).st_size - 1), 'POST', SynCurl.Upload, filepath)
        responses = json.loads(responses)
        if retcode != 200 or 'error_code' in responses:
            logger.error('Errno:%d: Upload file to pcs failed: %s, %s.' % (retcode, filepath, responses['error_msg']))
            return 1
        logger.info(' Upload file "%s" completed.' % (filepath))
        return 0
