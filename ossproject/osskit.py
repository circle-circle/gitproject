#coding:utf-8
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import base64
import hmac
import sha
import re
import Queue
import signal
import cPickle as pickle
from oss.oss_api import *
from oss.oss_xml_handler import *

PUT_OK = 0
GET_OK = 0
COPY_OK = 0
MAX_RETRY_TIMES = 3    

def check_res(res,msg=''):
    if  res.status /100 ==2:
        print '%s OK!' %msg
        print sep
    else:
        print "%s  FAIL" % msg
        print 'ret:%s' %res.status
        print 'request-id:%s' %res.getheader('x-oss-request-id')
        print 'reason:%s' %res.read()
        print sep
        choice_list()
        
def check_not_empty(input,msg=''):
    if not input:
        print "please make sure %s not empty" %msg
        exit(-1)

def format_datetime(osstimestamp):
    date = re.compile("(\.\d*)?Z").sub(".000Z", osstimestamp)
    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.000Z")
    return time.strftime("%Y-%m-%d %H:%M", ts)

def get_oss(show_bar = True):
    oss = OssAPI(OSS_HOST, ID, KEY)
    oss.show_bar = show_bar 
    oss.set_send_buf_size(SEND_BUF_SIZE)
    oss.set_recv_buf_size(RECV_BUF_SIZE)
    oss.set_debug(IS_DEBUG)
    return oss

def listallbuckets():
    res = oss.get_service()
    width = 20
    if (res.status / 100) == 2:
        body = res.read()
        h = GetServiceXml(body)
        is_init = False
        for i in h.list():
            if len(i) >= 3 and i[2].strip():
                if not is_init:
                    print "%s %s %s" % ("CreateTime".ljust(width), "BucketLocation".ljust(width), "BucketName".ljust(width))
                    is_init = True
                print "%s %s %s" % (str(format_datetime(i[1])).ljust(width), i[2].ljust(width), i[0])
            elif len(i) >= 2:
                if not is_init:
                    print "%s %s" % ("CreateTime".ljust(width), "BucketName".ljust(width))
                    is_init = True
                print "%s %s" % (str(format_datetime(i[1])).ljust(width), i[0])
        print "\nBucket Number is: ", len(h.list())
    return check_res(res,'list all buckets')

def copy_object(src_bucket, src_object, des_bucket, des_object, headers,totall, retry_times = 3):
    totall = totall
    replace = True
    global COPY_OK        
    for i in xrange(retry_times):
        tmp_headers = headers.copy()
        try:
            if replace:
                res = oss.copy_object(src_bucket, src_object, des_bucket, des_object, tmp_headers)
                if res.status == 200:
                    COPY_OK += 1 
                    print "%s / %s objects header are modified OK, marker is:%s" % (COPY_OK,totall, src_object)
                    return True
                else:
                    print "modify header /%s/%s  FAIL, status:%s, reason:%s" % \
                    (src_bucket, src_object, res.status, res.read())
            else:
                res = oss.head_object(des_bucket, des_object)
                if res.status == 200:
                    COPY_OK += 1
                    return True
                elif res.status == 404:
                    res = oss.copy_object(src_bucket, src_object, des_bucket, des_object, tmp_headers)
                    if res.status == 200:
                        COPY_OK += 1
                        return True
                    else:
                        print "modify  /%s/%s FAIL, status:%s, request-id:%s" % \
                        (src_bucket, src_object, res.status, res.getheader("x-oss-request-id"))
        except:
            print "modify /%s/%s to %s exception" % (src_bucket, src_object,headers)
   
def batch_modify_header(bucket,object):
    b = GetAllObjects()
    object_list = b.get_all_object_in_bucket(oss,bucket,prefix=object) 
    object_list = b.object_list  
    headers = {}    
    header_choice =['Content-Type','Cache-Control','Content-Disposition','Content-Encoding','Expires']
    objtotall = len(object_list)
    flag = 1
    while flag:
        header_input = raw_input("please input header choice:['Content-Type','Cache-Control','Content-Disposition','Content-Encoding','Content-Language','Expires'] or exit [save]:").title()
        if header_input == 'Content-Type':
            content_type = raw_input("Input Content-Type value:")
            headers['Content-Type'] = content_type
        elif header_input == 'Cache-Control':
            cache_control = raw_input("Input Cache-Control value:")
            headers['Cache-Control'] = cache_control
        elif header_input == 'Content-Disposition':
            content_disposition = raw_input("Input Content-Disponsition value:")
            headers['Content-Disposition'] = content_disposition
        elif header_input == 'Content-Encoding':
            content_encoding = raw_input("Input Content-Encoding value:")
            headers['Content-Encoding'] = content_encoding
        elif header_input == 'Content-Language':
            content_language = raw_input("Input Content-Language value:")
            headers['Content-Language'] = content_language
        elif header_input == 'Expires':
            expires = raw_input("Input expires value:")
            headers['Expires'] = expires
        elif header_input == 'Save':
            print 'save exit!'
            flag = 0
        else:
            print sep
            print "   !!!Please Input Correct String!!!   "
    print "Your header value is %s" %headers
    answer = raw_input('Are you sure y/n:').lower()
    if answer =='y':
        for  object  in  object_list:
            copy_object(bucket, object, bucket, object,headers,objtotall)
    else:
        return False

def copy_source_to_target(source_bucket,target_bucket):
    b = GetAllObjects()
    object_list = b.get_all_object_in_bucket(oss,source_bucket) 
    object_list = b.object_list 
    for object  in  object_list:
        res=oss.copy_object(source_bucket, object, target_bucket, object)
        check_res(res, object)
    

# to sum bucket size
def sum_bucket_size(bucket):  
    b = GetAllObjects()
    b.get_all_object_in_bucket(oss,bucket) 
    object_list = b.object_list
    object_num =len(object_list)
    print 'the bucket %s have %d objects' %(bucket,object_num)

    try:
        bucket_size = 0
        for object in object_list:           
            res=oss.head_object(bucket, object,headers=None)
            bucket_size += int(res.getheader('Content-Length'))
    except:
        print " to sum fail !"       
    print 'the bucket %s size is  %f MB' %(bucket,bucket_size/1024.0/1024)
    print sep

def changeaddress(hostnum):
    global oss
    if os.path.isfile(configfile) and file(configfile).readline != '':
        with file(configfile,'rb+') as f:
            D = pickle.load(f)
        ACCESS_ID = D['id']
        SECRET_ACCESS_KEY = D['key']
        if hostnum == '1':
            D['host']='oss-cn-hangzhou.aliyuncs.com'
        elif hostnum == '2':
            D['host']='oss-cn-hangzhou-internal.aliyuncs.com'
        elif hostnum == '3':
            D['host'] = 'oss-cn-qingdao.aliyuncs.com'
        elif hostnum == '4':
            D['host'] = 'oss-cn-qingdao-internal.aliyuncs.com'
        elif hostnum == '5':
            D['host'] = 'oss-cn-beijing.aliyuncs.com'
        elif hostnum == '6':
            D['host'] = 'oss-cn-beijing-internal.aliyuncs.com'
        elif hostnum == '7':
            D['host'] = 'oss-cn-hongkong.aliyuncs.com'
        elif hostnum == '8':
            D['host'] = 'oss-cn-hongkong-internal.aliyuncs.com'
        else :
            print u'没有输入正确的编号，地址没有改变'
        Host = D['host']
        with file(configfile,'wb+') as f:
            pickle.dump(D,f)
        oss = OssAPI(Host,ACCESS_ID,SECRET_ACCESS_KEY)
        print u'您现在位于 %s' %Host
        print sep
    else:
        print u'请先登录'
        zhuxiao()

class DownloadObjectWorker(threading.Thread):
    def __init__(self, retry_times, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.retry_times = retry_times

    def run(self):
        while 1:
            try:
                (get_object, bucket, object, object_prefix, local_path, length, last_modify_time, replace) = self.queue.get(block=False)
                get_object(bucket, object, object_prefix, local_path, length, last_modify_time, replace, self.retry_times)
                self.queue.task_done()
            except Queue.Empty:
                break
            except:
                self.queue.task_done()

def format_unixtime(osstimestamp):
    date = re.compile("(\.\d*)?Z").sub(".000Z", osstimestamp)
    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.000Z")
    return (int)(time.mktime(ts))

def get_object(bucket, object, object_prefix, local_path, length, last_modify_time, replace, retry_times = MAX_RETRY_TIMES):
    show_bar = True
    object = smart_code(object)
    tmp_object = object
    if object_prefix == object[:len(object_prefix)]:
        tmp_object = object[len(object_prefix):]
    while 1:
        if not tmp_object.startswith("/"):
            break
        tmp_object = tmp_object[1:]
    localfile = os.path.join(local_path, tmp_object) 
    localfile = smart_code(localfile)
    global GET_OK
    for i in xrange(retry_times):
        try:
            if os.path.isfile(localfile):
                t1 = format_unixtime(last_modify_time)
                t2 = (int)(os.path.getmtime(localfile))
                if not replace and (int)(length) == os.path.getsize(localfile) and t1 < t2:
                    print "no need to get %s/%s to %s" % (bucket, object, localfile)
                    GET_OK += 1
                    return True 
            else:
                try:
                    dirname = os.path.dirname(localfile)
                    if not os.path.isdir(dirname):
                        os.makedirs(dirname)
                except:
                    pass
            ret = False
            res = oss.get_object_to_file(bucket, object, localfile)
            if 200 == res.status:
                ret = True
            if ret and (int)(length) == os.path.getsize(localfile):
                GET_OK += 1
                print "get %s/%s to %s OK" % (bucket, object, localfile)
                return ret
            else:
                print "get %s/%s to %s FAIL" % (bucket, object, localfile)
        except:
            print "get %s/%s to %s exception" % (bucket, object, localfile)
    return False

def get_object_list_marker_from_xml(body):
    #return ([(object, object_length, last_modify_time)...], marker)
    object_meta_list = []
    next_marker = ""
    hh = GetBucketXml(body)
    (fl, pl) = hh.list()
    if len(fl) != 0:
        for i in fl:
            object = convert_utf8(i[0])
            last_modify_time = i[1]
            length = i[3]
            object_meta_list.append((object, length, last_modify_time))
    if hh.is_truncated:
        next_marker = hh.nextmarker
    return (object_meta_list, next_marker)

def downloadallobject(bucket, local_path):
    (bucket, object) = parse_bucket_object(bucket)
    if os.path.isfile(local_path):
        print "%s is not dir, please input localdir" % local_path
        exit(-1)
    replace = False
    prefix = object
    thread_num = 5
    retry_times = MAX_RETRY_TIMES
    marker = ""
    delimiter = ''
    maxkeys = '1000'
    handled_obj_num = 0
    while 1:
        queue = Queue.Queue(0)
        res = oss.get_bucket(bucket, prefix, marker, delimiter, maxkeys)
        if res.status != 200:
            return res
        body = res.read()
        (tmp_object_list, marker) = get_object_list_marker_from_xml(body)
        for i in tmp_object_list:
            object = i[0]
            length = i[1]
            last_modify_time = i[2]
            if str(length) == "0" and object.endswith("/"):
                continue
            handled_obj_num += 1 
            queue.put((get_object, bucket, object, prefix, local_path, length, last_modify_time, replace))
        thread_pool = []
        for i in xrange(thread_num):
            current = DownloadObjectWorker(retry_times, queue)
            thread_pool.append(current)
            current.start()
        queue.join()
        for item in thread_pool:
            item.join()
        if len(marker) == 0:
            break
    print u"下载的objects总数为: %s, 保存的路径在 %s" % (handled_obj_num, local_path)
    global GET_OK
    print "OK num:%s" % GET_OK
    fail_num = handled_obj_num - GET_OK
    print "FAIL num:%s" % (fail_num)
    if fail_num != 0:
        exit(0)

class UploadObjectWorker(threading.Thread):
    def __init__(self, check_point_file, retry_times, queue):
        threading.Thread.__init__(self)
        self.check_point_file = check_point_file
        self.queue = queue
        self.file_time_map = {}
        self.retry_times = retry_times
    def run(self):
        while 1:
            try:
                (put_object, bucket, object, local_file) = self.queue.get(block=False)
                ret = put_object(bucket, object, local_file, self.retry_times)
                if ret:
                    local_file_full_path = os.path.abspath(local_file)
                    local_file_full_path = format_utf8(local_file_full_path)
                    self.file_time_map[local_file_full_path] = (int)(os.path.getmtime(local_file))
                self.queue.task_done()
            except Queue.Empty:
                break
            except:
                self.queue.task_done()
        if len(self.file_time_map) != 0:
            dump_check_point(self.check_point_file, self.file_time_map)



lock = threading.Lock()
def dump_check_point(check_point_file, result_map=None):
    if len(check_point_file) == 0 or len(result_map) == 0:
        return
    lock.acquire()
    old_file_time_map = {}
    if os.path.isfile(check_point_file):
        old_file_time_map = load_check_point(check_point_file)
    try:
        f = open(check_point_file,"w")
        for k, v in result_map.items():
            if old_file_time_map.has_key(k) and old_file_time_map[k] < v:
                del old_file_time_map[k]
            line = "%s#%s\n" % (v, k)
            line = format_utf8(line)
            f.write(line)
        for k, v in old_file_time_map.items():
            line = "%s#%s\n" % (v, k)
            line = format_utf8(line)
            f.write(line)
    except:
        pass
    try:
        f.close()
    except:
        pass
    lock.release()

def load_check_point(check_point_file):
    file_time_map = {}
    if os.path.isfile(check_point_file):
        f = open(check_point_file)
        for line in f:
            line = line.strip()
            tmp_list = line.split('#')
            if len(tmp_list) > 1:
                time_stamp = (float)(tmp_list[0])
                time_stamp = (int)(time_stamp)
                file_name = "".join(tmp_list[1:])
                file_name = format_utf8(file_name)
                if file_time_map.has_key(file_name) and file_time_map[file_name] > time_stamp:
                    continue
                file_time_map[file_name] = time_stamp
        f.close()
    return file_time_map

def format_utf8(string):
    string = smart_code(string)
    if isinstance(string, unicode):
        string = string.encode('utf-8')                
    return string

def format_object(object):
    tmp_list = object.split(os.sep)
    object = "/".join(x for x in tmp_list if x.strip() and x != "/")
    while 1:
        if object.find('//') == -1:
            break
        object = object.replace('//', '/')
    return  object

def check_localfile(localfile):
    if not os.path.isfile(localfile):
        print "%s is not existed!" % localfile
        sys.exit(1)

def split_path(path):
    pather = path.strip().split('/')
    return pather

def parse_bucket_object(path):
    pather = split_path(path)
    bucket = ""
    object = ""
    if len(pather) > 0:
        bucket = pather[0]
    if len(pather) > 1:
        object += '/'.join(pather[1:])
    object = smart_code(object)
    if object.startswith("/"):
        print "object name SHOULD NOT begin with /"
        sys.exit(1)
    return (bucket, object)


def put_object(bucket, object, local_file, retry_times=5):
    oss.show_bar = True
    limit_size = 5*1024*1024*1024
    for i in xrange(retry_times):
        try:
            object = smart_code(object)
            local_file = smart_code(local_file)
            local_file_size = os.path.getsize(local_file)
            if local_file_size > limit_size:
                upload_id = ""
                thread_num = 5
                max_part_num = 10000
                headers = {}
                res = oss.multi_upload_file(bucket, object, local_file, upload_id, thread_num, max_part_num, headers)
            else:
                res = oss.put_object_from_file(bucket, object, local_file)
            if 200 == res.status:
                global PUT_OK
                PUT_OK += 1
                print "upload %s OK" % (local_file)
                return True 
            else:
                print "upload %s FAIL, status:%s, reason:%s" % (local_file, res.status, res.read())
        except:
            print "put %s/%s from %s exception" % (bucket, object, local_file)
    return False

def upload_object_from_localdir(check_point_file,local_path, path):
    (bucket, object) = parse_bucket_object(path)
    if not os.path.isdir(local_path):
        print u"%s 非目录, 请输入需要上传的目录路径" % local_path
        return
    
    prefix = object
    retry_times = 5
    thread_num = 5

    topdown = True
    queue = Queue.Queue(0)
    local_path = smart_code(local_path)
    def process_localfile(items):
        for item in items:
            local_file = os.path.join(root, item) 
            local_file = smart_code(local_file)
            if os.path.isfile(local_file):
                if prefix != "":
                    object = prefix + "/" + local_file[len(local_path) + 1:]
                else:
                    object = local_file[len(local_path) + 1:]
                object = format_object(object)
                queue.put((put_object, bucket, object, local_file))

    for root, dirs, files in os.walk(local_path, topdown):
        process_localfile(files)
        process_localfile(dirs)
                 
    qsize = queue.qsize()
    thread_pool = []
    for i in xrange(thread_num):
        current = UploadObjectWorker(check_point_file, retry_times, queue)
        thread_pool.append(current)
        current.start()
    queue.join()
    for item in thread_pool:
        item.join()

    print u"总共被上传的文件数: %s" % qsize
    global PUT_OK
    print u"上传成功的文件数:%s" % PUT_OK
    print u"上传失败的文件数:%s" % (qsize - PUT_OK)
    print sep

def zhuxiao():
    #注销
    if os.path.exists(configfile):
        os.remove(configfile)
    if os.path.exists(checkpointfile):
        os.remove(checkpointfile)
    sys.exit()

def choice_list():
    while 1:
        choice = raw_input((u'''
                1)列出所有bucket
                2)统计bucket下的object数量以及bucket的总大小
                3)批量修改bucket下所有object的HTTP头信息
                4)清除bucket 
                5)批量上传文件  
                6)迁移bucket   同节点object数量少于1000迁移
                7)批量下载文件
                
                0)退出
                00)注销
                000)更换节点
                
                请输入需要操作的数字编号:'''))
        if choice == '1':
            listallbuckets()
        elif choice == '2':
            bucket = raw_input(u'请输入需要统计的bucket名称:')
            sum_bucket_size(bucket)
        elif choice == '3':
            path = raw_input(u'请输入需要修改主机头的bucket路径:')
            (bucket, object) = parse_bucket_object(path)
            batch_modify_header(bucket,object)
        elif choice == '4':
            bucket = raw_input(u'请输入需要删除所有objects的bucket名称:')
            print u'请确认要删除的bucket为 %s，删除后将无法恢复' %bucket
            answer = raw_input('Are you sure y/n:').lower()
            if answer =='y':
                clear_all_object_of_bucket(oss,bucket)  
            else:
                print u'请重新选择操作'
                print sep 
        elif choice == '5':
            check_point_file = checkpointfile
            localpath = raw_input(u'请输入需要批量上传文件所在的目录路径:')
            path = raw_input(u'请输入需要上传至bucket的名称路径:')
            upload_object_from_localdir(check_point_file,localpath,path)
        elif choice  == '6':
            source_bucket = raw_input(u'请输入需要迁移的源bucket名称:')
            source_bucket_location = oss.get_bucket_location(source_bucket)
            print source_bucket_location.read()
            target_bucket = raw_input(u'请输入需要迁移的目的bucket名称:')
            target_bucket_location = oss.get_bucket_location(target_bucket)
            print target_bucket_location.read()
            copy_source_to_target(source_bucket,target_bucket)
        elif choice == '7':
            bucket = raw_input(u'请输入需要下载文件所在的bucket路径:')
            local_path = raw_input(u'请输入文件保存的路径:')
            downloadallobject(bucket, local_path)
        elif choice == '000':
            hostnum = raw_input(u'''
                address list  
                杭州 1: oss-cn-hangzhou.aliyuncs.com
                杭州内网地址 2: oss-cn-hangzhou-internal.aliyuncs.com
                青岛 3: oss-cn-qingdao.aliyuncs.com
                青岛内网地址 4: oss-cn-qingdao-internal.aliyuncs.com
                北京 5：oss-cn-beijing.aliyuncs.com
                北京内网地址 6：oss-cn-beijing-internal.aliyuncs.com
                香港 7：oss-cn-hongkong.aliyuncs.com
                香港内网地址 8: oss-cn-hongkong-internal.aliyuncs.com

                请输入您需要切换的节点数字编号:''')
            changeaddress(hostnum)

        elif choice == '0':
            sys.exit(0)
        elif choice == '00':
            zhuxiao()
        else:
            print u"请输入您需要操作的数字！"
            print sep

if __name__ == '__main__':
    PUT_OK = 0
    address = ['oss-cn-qingdao.aliyuncs.com','oss-cn-hangzhou.aliyuncs.com']
    configfile = os.path.expanduser('~') + '/.osscredential'
    checkpointfile = os.path.expanduser('~') + '/.checkpointfile.txt'
    sep = "-"*66
    #判断上次是否有注销行为,若没有则直接登录（搜索用户主目录下的配置文件），否则重新输入id,key以及host
    if os.path.isfile(configfile):
        with file(configfile,'rb') as f:
            D = pickle.load(f)
        Host = D['host']
        ACCESS_ID = D['id']
        SECRET_ACCESS_KEY = D['key']
        oss = OssAPI(Host,ACCESS_ID,SECRET_ACCESS_KEY)
        print '      now you are in %s' %Host
        choice_list()
    else:
        ACCESS_ID = raw_input('please input your ACCESS_ID:')
        check_not_empty(ACCESS_ID,'ACCESS_ID')
        SECRET_ACCESS_KEY = raw_input('please input your SECRET_ACCESS_KEY:')
        check_not_empty(SECRET_ACCESS_KEY,'SECRET_ACCESS_KEY')    
        Hostnum = raw_input(u'''
                address list  
                杭州 1: oss-cn-hangzhou.aliyuncs.com
                杭州内网地址 2 : oss-cn-hangzhou-internal.aliyuncs.com
                青岛 3 : oss-cn-qingdao.aliyuncs.com
                青岛内网地址 4: oss-cn-qingdao-internal.aliyuncs.com               
                北京 5：oss-cn-beijing.aliyuncs.com
                北京内网地址 6：oss-cn-beijing-internal.aliyuncs.com
                香港 7：oss-cn-hongkong.aliyuncs.com
                香港内网地址 8: oss-cn-hongkong-internal.aliyuncs.com
                请输入节点对应的编号(默认为杭州节点):''')
        if Hostnum == '1':
            Host ='oss-cn-hangzhou.aliyuncs.com'
        elif Hostnum == '2':
            Host ='oss-cn-hangzhou-internal.aliyuncs.com'
        elif Hostnum == '3':
            Host = 'oss-cn-qingdao.aliyuncs.com'
        elif Hostnum == '4':
            Host = 'oss-cn-qingdao-internal.aliyuncs.com'
        elif Hostnum == '5':
            Host = 'oss-cn-beijing.aliyuncs.com'
        elif Hostnum == '6':
            Host = 'oss-cn-beijing-internal.aliyuncs.com'
        elif Hostnum == '7':
            D['host'] = 'oss-cn-hongkong.aliyuncs.com'
        elif Hostnum == '8':
            D['host'] = 'oss-cn-hongkong-internal.aliyuncs.com'
        else :
            Host = 'oss-cn-hangzhou.aliyuncs.com'
        print sep
        oss = OssAPI(Host,ACCESS_ID,SECRET_ACCESS_KEY)
        D = { 'host':Host,'id':ACCESS_ID,'key':SECRET_ACCESS_KEY }
        with file(configfile,'wb+') as f:
            pickle.dump(D,f)
        choice_list()
    









