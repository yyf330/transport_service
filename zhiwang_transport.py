import redis
#import psycopg2
import threading
import time
import sys
import socket
import struct
import multiprocessing

#print(len(sys.argv))
if len(sys.argv)<2:
    db_select='0'
else:
    db_select=sys.argv[1]
print(db_select)
r = redis.Redis(db=str(db_select),password='admin')
# print("redis connect")
exit_flag=False

# conn = psycopg2.connect(database="dss", user="dssadmin", password="dssadmin", host="127.0.0.1", port="5432")

def redis_to_ps(args):
    # print("thread  redis_to ps  start!")
    while True:
        global exit_flag
        if exit_flag:
            print("exit")
            break
        # time1=time.time()
        # print('spent_time1=', time.clock())
        if not (r.exists("capture_xid")):
            # print("no xid!")
            #time2 = time.time()
            # print('spent_time2=', time.clock())
            time.sleep(1)
            continue
        xid_key=r.rpop("capture_xid")
        #print("xid=",xid_key)
        pid_results = []
        pid_results = r.lrange(xid_key,0,-1)
        # print("pid_results=",pid_results)
        pid_results.sort()
        commit_flag = False
        rollback_flag = False
        if len(pid_results)!=0:
            for key in pid_results:

                hmlist=r.hmget(key,["scn","TIMESTAMP","operation_code"])
                # print(hmlist)
                if hmlist[2] is None:
                    continue
                operation_code=bytes.decode(hmlist[2])
                #print(key)
                #print(operation_code)
                # print("pid_results=",pid_results)
                if operation_code=='6':
                    r.delete(key)
                    r.lrem(xid_key, key, 1)
                    continue
                elif operation_code=='7':

                    if commit_flag:
                        r.lpush("capture_commit", xid_key)
                    else:
                        for key_delete in pid_results:
                            # print("lajikey=", key_delete)
                            r.delete(key_delete)
                            r.lrem(xid_key, key_delete, 1)
                        # print("lajikey=",key)
                        # r.lrem(xid_key, key, 1)
                        # r.delete(key)
                    break
                elif operation_code=='36':
                    # rollback_flag = True
                    # r.delete(key)
                    # r.lrem(xid_key, key, 1)
                    r.lpush("xid_rollback", xid_key)

                    break
                else:
                    commit_flag=True
                    continue
                    # r.delete(bytes.decode(key))
            # if rollback_flag:
            #     for key_delete in pid_results:
            #         r.delete(key_delete)
            #         r.lrem(xid_key, key_delete, 1)

            pid_results=[]
        if exit_flag:
            break


def rollback_to_delete(args):
    # print("thread  redis_to ps  start!")
    while True:
        # time1=time.time()
        # print('spent_time1=', time.clock())
        if not (r.exists("xid_rollback")):
            # print("no xid!")
            #time2 = time.time()
            # print('spent_time2=', time.clock())
            time.sleep(1)
            continue
        xid_key=r.rpop("xid_rollback")
        # print("xid=",xid_key)

        while r.exists(xid_key):
            key = r.rpop(xid_key)
            r.delete(key)


# def scan_to_delete(args):
#     while True:
#         mid_results = []
#         cur, results = r.scan(cursor=0, match=('xid:*'))
#         mid_results += results
#         while cur != 0:
#             cur, results = r.scan(cursor=0, match=('xid:*'))
#             mid_results += results

def heart_push(args):
    while True:
        global exit_flag
        # print("thread  heart_push start!",exit_flag)
        if exit_flag:
            break
        port = 8686
        host = 'localhost'
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        time_str=time.strftime("%Y%m%d%H%M%S", time.localtime())
        # //# 201708041448200786#005#2;1;#
        msg='#'+time_str+'#003#'+ db_select+';1;#'
        # print(msg)
        s.sendto(msg.encode('utf8'), (host, port))
        time.sleep(5)
        if exit_flag:
            break

def msg_check(args):
    while True:
        global exit_flag
        global db_select
        # print("thread  msg_check start!",exit_flag)
        HOST = ''  # Symbolic name meaning all available interfaces
        PORT = 9400+ int(db_select) # Arbitrary non-privileged port

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # print ('Socket created')
        try:
            s.bind((HOST, PORT))
        except Exception as e:
            print("bind error!")
        # print ('Socket bind complete')
        s.listen(10)
        # print('Socket now listening')

        # wait to accept a connection - blocking call
        conn, addr = s.accept() #wait here when no msg
        # print ('Connected with ' + addr[0] )

        # now keep talking with the client
        data = conn.recv(2048)
        len_buf = len(data)
        # Size = struct.calcsize('2i24s')
        # print("size=",Size)
        msg_type,cmd_type,source_id= struct.unpack('3i',data[:12])
        # bf = ""
        # bf = bytes(bf, 'UTF-8')
        # bf = struct.unpack("13s", data[8:21])[0]
        # bb = bf.decode('utf-8')
        # print(bb)

        if msg_type==0x5c:
            if cmd_type==0x13:
                exit_flag=True
                break
        else:
            print ('cmd_type=',cmd_type)

        # print (struct.unpack("ii", data)[0])
        # if struct.unpack("ii",data)[0]==0x5c:
        # print("ok")
        # print (struct.unpack("ii", data)[1])
        conn.sendall(b'ok!')
        conn.close()
        s.close()


threads = []
t1 = threading.Thread(target=redis_to_ps,args=('',))
threads.append(t1)
# t2 = threading.Thread(target=heart_push,args=('',))
# threads.append(t2)
t3 = threading.Thread(target=msg_check,args=('',))
threads.append(t3)

if __name__== "__main__":
    p_list=[rollback_to_delete,heart_push]
    for pro in p_list:
        p = multiprocessing.Process(target = pro, args = ('',))
        p.daemon = True
        p.start()

    try:
        for t in threads:
            t.setDaemon(True)
            t.start()
        t.join()
    except (KeyboardInterrupt,SystemExit):
        print ("exit!")

