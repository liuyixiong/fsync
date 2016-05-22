#!/usr/bin/env python3

from fsync.baidupcsapi import BaiduPcsApi
from fsync.conf import SynConfig
import json
import hashlib
import os

testdir = SynConfig.config['remotepath']+'/tests/pcsapi'

def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()

def test_quota():
    baidu = BaiduPcsApi()
    r = baidu.get_pcs_quota()
    assert r == 0

def test_mkdir_cp_mv_rm():
    baidu = BaiduPcsApi()
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    r = baidu.check_create_pcsdir(testdir+"/aa")
    assert r == 0
    r = baidu.cp_pcsfile(testdir+"/aa", testdir+"/bb")
    assert r == 0
    r = baidu.mv_pcsfile(testdir+"/aa", testdir+"/cc")
    assert r == 0
    r = baidu.rm_pcsfile(testdir+"/bb")
    assert r == 0
    r = baidu.rm_pcsfile(testdir+"/cc")
    assert r == 0
    r = baidu.create_pcsdir(testdir+"/aa")
    assert r == 0
    r = baidu.rm_pcsfile(testdir+"/aa")
    assert r == 0

def test_get_filemeta():
    baidu = BaiduPcsApi()
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    r = baidu.check_create_pcsdir(testdir+"/aa")
    assert r == 0
    filemeta = baidu.get_pcs_filemeta(testdir+"/aa")
    assert filemeta[0] == 0
    assert filemeta[1]["isdir"] == 1
    r = baidu.rm_pcsfile(testdir+"/aa")
    assert r == 0

def test_get_filelist():
    baidu = BaiduPcsApi()
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    r = baidu.check_create_pcsdir(testdir+"/aa")
    assert r == 0
    filelist = baidu.get_pcs_filelist(testdir, 0, 100)
    assert filelist[0] == 0
    assert len(filelist[1]) == 1
    assert filelist[1][0]["path"] == testdir+"/aa"
    r = baidu.rm_pcsfile(testdir+"/aa")
    assert r == 0

def test_upload_file():
    baidu = BaiduPcsApi()
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    r = baidu.upload_file('./tests/aaa.txt', testdir+"/aaa.txt")
    assert r == 0
    md5 = md5sum("./tests/aaa.txt")
    filemeta = baidu.get_pcs_filemeta(testdir+"/aaa.txt")
    assert filemeta[0] == 0
    assert filemeta[1]["isdir"] == 0
    block_list = json.loads(filemeta[1]["block_list"])
    assert block_list[0] == md5
    r = baidu.rm_pcsfile(testdir+"/aaa.txt")
    assert r == 0

def test_download_file():
    baidu = BaiduPcsApi()
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    r = baidu.upload_file('./tests/aaa.txt', testdir+"/aaa.txt")
    assert r == 0
    md5 = md5sum("./tests/aaa.txt")
    filemeta = baidu.get_pcs_filemeta(testdir+"/aaa.txt")
    assert filemeta[0] == 0
    assert filemeta[1]["isdir"] == 0
    with open("./tests/bbb.txt", 'w') as dlfn:
        pass
    r = baidu.download_file('./tests/bbb.txt', testdir+"/aaa.txt", "0-%d" % filemeta[1]['size'])
    assert r == 0
    r = baidu.rm_pcsfile(testdir+"/aaa.txt")
    assert r == 0
    os.remove("./tests/bbb.txt")

def test_rapid_uploadfile():
    baidu = BaiduPcsApi()
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    r = baidu.rapid_uploadfile('./tests/aaa.txt', testdir+"/aaa.txt")
    assert r == 0
    md5 = md5sum("./tests/aaa.txt")
    filemeta = baidu.get_pcs_filemeta(testdir+"/aaa.txt")
    assert filemeta[0] == 0
    assert filemeta[1]["isdir"] == 0
    block_list = json.loads(filemeta[1]["block_list"])
    assert block_list[0] == md5
    r = baidu.rm_pcsfile(testdir+"/aaa.txt")
    assert r == 0

def test_sclie_uploadfile():
    param = { "block_list":[] }
    baidu = BaiduPcsApi()
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    filesize = os.stat("./tests/aaa.txt").st_size
    scliesize= filesize//3
    r, md5 = baidu.slice_upload_tmpfile("./tests/aaa.txt", "0-%d" % scliesize)
    assert r == 0
    print(md5)
    param["block_list"].append(md5)
    r, md5 = baidu.slice_upload_tmpfile("./tests/aaa.txt", "%d-%d" % (scliesize+1, 2*scliesize))
    assert r == 0
    print(md5)
    param["block_list"].append(md5)
    r, md5 = baidu.slice_upload_tmpfile("./tests/aaa.txt", "%d-%d" % (2*scliesize+1, filesize-1))
    assert r == 0
    print(md5)
    param["block_list"].append(md5)
    print(param)
    r = baidu.slice_upload_createsuperfile(testdir+"/aaa.txt", param)
    assert r == 0

if __name__ == "__main__":
    test_quota()
    test_mkdir_cp_mv_rm()
    test_get_filemeta()
    test_get_filelist()
    test_upload_file()
    test_download_file()
    #test_rapid_uploadfile()
    #test_sclie_uploadfile()
