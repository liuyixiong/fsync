#!/usr/bin/env python3

from fsync.baidupcsapi import BaiduPcsApi
from fsync.conf import SynConfig

testdir = SynConfig.config['remotepath']+'/tests/pcsapi'
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

def test_upload_file_nosync():
    baidu = BaiduPcsApi()
    r = baidu.get_pcs_quota()
    assert r == 0
    r = baidu.check_create_pcsdir(testdir)
    assert r == 0
    r = baidu.upload_file_nosync('./tests/aaa.txt', testdir+"/aaa.txt")
    assert r == 0

    filemeta = baidu.get_pcs_filemeta(testdir+"/aaa.txt")
    assert filemeta[0] == 0
    assert filemeta[1]["isdir"] == 0
    r = baidu.rm_pcsfile(testdir+"/aaa.txt")
    assert r == 0

if __name__ == "__main__":
    test_quota()
    test_mkdir_cp_mv_rm()
    test_get_filemeta()
    test_get_filelist()
    test_upload_file_nosync()
