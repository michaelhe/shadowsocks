#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2017 michael(mching.08@gmail.com)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import socket
import struct
import time
import threading
import logging
import json

# the heart beat time
TIMEOUT_HB = 30
# the retry time
TIMEOUT_RETRY = 30

class MgrProxyHeader(object):
    """
    定义包的结构，使用struct来打包
    [version, body_len, cmd]
    version : 版本号
    body_len : 为后面的文本的长度
    cmd : 为指令ID
    cmd-404  : 不合法的回复
    cmd-1001 : 为PING报文
    cmd-1002 : 为PONG报文
    cmd-2000 : 为client->server上报的数据报文
    cmd-3000 : 为server->client下发的控制报文
    """

    version = 1
    header_fmt = '!3I'

    @classmethod
    def get_head_size(cls):
        return struct.calcsize(cls.header_fmt)

    @classmethod
    def pack_head(cls, data, cmd):
        body_len = str(data).__len__()
        header = [cls.version, body_len, cmd]
        return struct.pack(cls.header_fmt, *header)

    @classmethod
    def unpack_head(cls, data):
        return struct.unpack(cls.header_fmt, data)


class HeartBeatThd(threading.Thread):

    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.sock = sock

    def run(self):
        logging.info('start hb thread')
        try:
            while True:
                logging.debug('send ping...')
                send_msg = b'PING'
                send_cmd = 1001
                send_header = MgrProxyHeader.pack_head(send_msg, send_cmd)
                self.sock.sendall(send_header+send_msg)
                time.sleep(TIMEOUT_HB)
        except Exception,e:
            logging.error('HB thread found exception : %s' % e)
            self.sock.close()
            logging.error('%s thread is dead' % self.name)

class MgrProxy(threading.Thread):

    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.host = host
        self.port = port
        self._head_size = MgrProxyHeader.get_head_size()
        self._data_buffer = bytes()


    def doConnect(self):
        logging.info('Connect to %s:%s...' % (self.host, self.port))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(60)
        self.sock.connect((self.host, self.port))
        logging.info('Connected , sock is %s ' % self.sock )

    def doPing(self):
        logging.info('start to ping thread...')
        
        #self.pingthd = HeartBeatThd(self.sock)
        #self.pingthd.start()

        for i in range(0, 1):
            HeartBeatThd(self.sock).start()

    def waitControl(self):
        logging.info('start to control thread...')  

        while True:
            logging.debug('waiting for control message ...')
            msg = self.sock.recv(1024)
            logging.debug('receive message : %s' %  msg)

            if not msg:
                self.retry()

            if msg:
                self._data_buffer += msg
                while True:
                    if len(self._data_buffer) < self._head_size:
                        break
                    head_pack = MgrProxyHeader.unpack_head(
                        self._data_buffer[:self._head_size])

                    body_size = head_pack[1]

                    if len(self._data_buffer) < self._head_size + body_size :
                        break
                        
                    body = self._data_buffer[self._head_size:self._head_size + body_size]

                    self.handle_msg(head_pack, body)

                    self._data_buffer = self._data_buffer[self._head_size + body_size:]

        logging.info( 'main thread is gone')
            

    def handle_msg(self, head_pack, body):

        if head_pack[2] == 1002:
            logging.debug('Get response : %s ...' % body)
            pass

    def send_data(self, dict_data):
        # dict_data = { port : flow_int }
        logging.debug('send stat(%s) to server' % dict_data)
        send_msg = json.dumps(dict_data)
        send_cmd = 2000
        send_header = MgrProxyHeader.pack_head(send_msg, send_cmd)        
        self.sock.sendall(send_header+send_msg)

    def run(self):
        try:
            self.doConnect()
            self.doPing()
            self.waitControl()           
        except Exception,e:
            logging.info('MgrProxy run exception : %s' % e)
            self.retry()
        
    def retry(self):
        logging.error('lose connect, wait 5s and retry...') 
        self.delete()
        time.sleep(TIMEOUT_RETRY)
        self.run()   

    def delete(self):
        logging.info('delete this client ... ')
        self.sock.close()
        self._data_buffer = bytes()

if __name__=='__main__':
    try:
        mgr_proxy = MgrProxy('linode.ylkb.net',6221)
        mgr_proxy.run()
    except KeyboardInterrupt:
        logging.error('Found KeyboardInterrupt...')
        mgr_proxy.delete()
    except Exception,e:
        logging.error('found exception : %s' % e)
        mgr_proxy.delete()
        

