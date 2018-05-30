import socket
import fix
import logging
import sys
import colorama
from colorama import Back, Style
from collections import OrderedDict
from fix.util import ch_delim, iter_rawmsg

colorama.init()


def set_keepalive_linux(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    """Set TCP keepalive on an open socket.

    It activates after 1 second (after_idle_sec) of idleness,
    then sends a keepalive ping once every 3 seconds (interval_sec),
    and closes the connection after 5 failed ping (max_fails), or 15 seconds
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


class UnexpectedMessageException(Exception):

    def __init__(self, message='', *, offending_msg):
        detail = '(unexpected message: {})'.format(offending_msg)
        if message:
            detail = f'{message} {detail}'
        super().__init__(detail)
        self.offending_msg = offending_msg


class NoMessageResponseException(Exception):

    def __init__(self, message=''):
        detail = '(no message received)'
        if message:
            detail = f'{message} {detail}'
        super().__init__(detail)


class FixClient():

    def __init__(self, config=None, *, conn_name='default', timeout=5,
                 auto=True, verbose=1, log_level=logging.INFO,
                 filter_tags=None):
        self.config = config
        self.auto = auto
        self.seqnum = 1
        self.conn_name = conn_name
        self.ip = config[conn_name]['OMSIP']
        self.port = config[conn_name].getint('OMSPort')
        self.targetcompid = config[conn_name]['OMSTarget'].encode()
        self.sendercompid = config[conn_name]['OMSSender'].encode()
        self.beginstring = config[conn_name]['BeginString'].encode()
        self.heartbeat = config[conn_name]['OMSHeartBeat'].encode()
        self.header_fill = {8: self.beginstring,
                            49: self.sendercompid,
                            56: self.targetcompid}

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logged_on = False
        self.verbose = verbose
        self.filter_tags = filter_tags or {8, 9, 49, 56, 52, 10, 60, 11, 43, 97}
        #self.filter_tags = filter_tags or {8, 9, 49, 56, 52, 10, 60, 43, 97}
        self.timeout = timeout

        self.log = logging.getLogger(f'FixClient-{conn_name}')
        self.log.setLevel(log_level)
        formatter = logging.Formatter(
            Back.BLACK +
            '[%(asctime)s - %(name)s - %(levelname)s]' +
            Style.RESET_ALL +
            ' %(message)s'
            #  , datefmt='%Y%m%d %H:%M:%S'
        )
        self.ch = logging.StreamHandler(sys.stdout)
        self.ch.setLevel(logging.DEBUG)
        self.ch.setFormatter(formatter)
        self.log.addHandler(self.ch)

    def seq(self, no_raise=False):
        if not no_raise and not self.logged_on:
            raise Exception(
                'Next sequence number when not logged on can be meaningless. '
                'Use no_raise kwarg to turn this exception off')
        a = self.seqnum
        self.seqnum += 1
        return a

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.ip, self.port))
        self.sock.settimeout(self.timeout)

    def reconnect(self):
        self.sock.close()
        self.logged_on = False
        self.seqnum = 1
        self.connect()
        if self.auto:
            self.logon_recv_response()

    def close(self):
        self.sock.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def logon_recv_response(self):
        self.logon()
        while True:
            rmsg = self.recv_fix(log_level=logging.DEBUG)
            prmsg = fix.Message.parse(rmsg)
            if prmsg.msgtype == b'A':
                self.log.info('logged on response rcvd')
                break
            elif prmsg.msgtype == b'1':
                self.log.debug(
                    f'<<: testreq {ch_delim(rmsg)}, sending heartbeat')
                self.send_heartbeat(prmsg[112])
            elif prmsg.msgtype == b'2':
                raise UnexpectedMessageException(
                    'seqnum is off', offending_msg=prmsg)
            else:
                raise UnexpectedMessageException(
                    'non logon response', offending_msg=prmsg)

    def logout_recv_response(self):
        self.logout()
        while True:
            rmsg = self.recv_fix(log_level=logging.DEBUG)
            prmsg = fix.Message.parse(rmsg)
            if prmsg.msgtype == b'8':
                self.log.info(f'ack msg for others rcvd {prmsg}')
            elif prmsg.msgtype == b'5':
                self.log.info(f'logged out response rcvd {prmsg}')
                self.logged_on = False
                break
            elif prmsg.msgtype == b'1':
                self.log.debug(
                    f'<<: testreq {ch_delim(rmsg)}, sending heartbeat')
                self.send_heartbeat(prmsg[112])
            elif prmsg.msgtype == b'2':
                raise UnexpectedMessageException(
                    'seqnum is off', offending_msg=prmsg)
            else:
                raise UnexpectedMessageException(
                    'non logout response', offending_msg=prmsg)

    def __enter__(self):
        self.connect()
        self.seqnum = 1
        if self.auto:
            self.logon_recv_response()
        return self

    def __exit__(self, *args, no_raise=True):
        if self.auto:
            try:
                self.logout_recv_response()
            except:
                if (no_raise==True):
                   print ('got exception {} during log out'.format(sys.exc_info()))
                else:
                   raise Exception('got exception {}', sys.exc_info())
            #self.close()
    def send_msg(self, msg: bytes, log_level=logging.INFO):
        self.sock.sendall(msg)
        if self.filter_tags:
            filtered_bmsg = b'| '.join(
                t + b': ' + v for t, v in iter_rawmsg(msg)
                if int(t.decode()) not in self.filter_tags)
            self.log.log(log_level, f'>>: {filtered_bmsg}')
        else:
            self.log.log(log_level, f'>>: {ch_delim(msg)}')
        with open('traffic.log','a+') as traffic_log:
             traffic_log.write('client sent >> OMS session:' + msg.decode())
             traffic_log.write('\n')
             traffic_log.close()

    def send_recv(self, msg: bytes):
        self.send_msg(msg)
        return self.recv_fix()

    def new_msg(self, msgtype_cls: type, extra: OrderedDict=None, seq=True,
                **kwargs):
        d = OrderedDict({**self.header_fill})
        if seq:
            d[34] = str(self.seq()).encode()
        d.update(extra)
        return msgtype_cls(fix.Group(d), **kwargs)

    def recv_fix(self, *, up_to_tag9_anchor_len=22, log_level=logging.INFO):
        msg_recv1 = self.sock.recv(up_to_tag9_anchor_len)
        if not msg_recv1:
            raise NoMessageResponseException()

        def get_bodylen(msg: bytes):
            after_tag9_equal_sign = msg.partition(
                b'\x01')[-1].partition(b'=')[-1]
            len_bytes, _, extra = after_tag9_equal_sign.partition(b'\x01')
            return int(len_bytes.decode()), len(extra)
        # also get the 7 byte checksum
        body_len, extra = get_bodylen(msg_recv1)
        msg_recv2 = self.sock.recv(body_len + 7 - extra)
        msg = msg_recv1 + msg_recv2
        if self.filter_tags:
            filtered_bmsg = b'| '.join(
                t + b': ' + v for t, v in iter_rawmsg(msg)
                if int(t.decode()) not in self.filter_tags)
            self.log.log(log_level, f'<<: {filtered_bmsg}')
        else:
            self.log.log(log_level, f'<<: {ch_delim(msg)}')
        with open('traffic.log','a+') as traffic_log:
             traffic_log.write('OMS sent >> client session:' + msg.decode())
             traffic_log.write('\n')
             traffic_log.close()
        return msg

    # use the order id & seqnum to find the corresponding ack, once got
    # irrelevant msg, just ignores it.
    def recv_linked_ack_dic(self, org_ordId, org_seqNum):
        n = self.recv_fix()
        tmpIncomingMsgDic = fix.Message.parse(n)
        self.log.debug('got the following msg', tmpIncomingMsgDic)
        try:
            tmpIncomingMsg_orderID = tmpIncomingMsgDic[11]
        except:            
            return self.recv_linked_ack_dic(org_ordId, org_seqNum)           
        if tmpIncomingMsg_orderID!=org_ordId:
            return self.recv_linked_ack_dic(org_ordId, org_seqNum)
        if int(tmpIncomingMsgDic[34])<int(org_seqNum) and tmpIncomingMsg_orderID==org_ordId:
            return self.recv_linked_ack_dic(org_ordId, org_seqNum) 
        return tmpIncomingMsgDic


    # use the order id & seqnum to find the corresponding ack, once got irrelevant msg, just ignores it.
    def recv_linked_ack_use_id (self, org_ordId):
        n = self.recv_fix()
        tmpIncomingMsgDic2 = fix.Message.parse(n)
        try:
            tmpIncomingMsg_orderID = tmpIncomingMsgDic2[11]
        except:
            return self.recv_linked_ack_use_id(org_ordId)

        if tmpIncomingMsg_orderID!=org_ordId:
            return self.recv_linked_ack_use_id(org_ordId) 
        else:
            return tmpIncomingMsgDic2        
    
    def recv_linked_ack_use_clientorderid (self, org_ordId):
        n = self.recv_fix()
        tmpIncomingMsgDic = fix.Message.parse(n)
        try:
            tmpIncomingMsg_orderID = tmpIncomingMsgDic[37]
        except:
            return self.recv_linked_ack_use_clientorderid(org_ordId)
        if tmpIncomingMsg_orderID!=org_ordId:
            return self.recv_linked_ack_use_clientorderid(org_ordId)
        else:
            return tmpIncomingMsgDic
    def logon(self):
        self.log.info('logging on...')
        logon_msg = fix.LogonMessage(
            fix.Group({**self.header_fill,
                       34: str(self.seq(no_raise=True)).encode(),
                       108: self.heartbeat})
        )
        self.send_msg(bytes(logon_msg), log_level=logging.DEBUG)
        self.logged_on = True

    def logout(self):
        self.log.info('logging out...')
        logout_msg = fix.LogoutMessage(
            fix.Group({**self.header_fill,
                       34: str(self.seq(no_raise=True)).encode(),
                       })
        )
        self.send_msg(bytes(logout_msg), log_level=logging.DEBUG)
        #self.logged_on = False

    def send_heartbeat(self, testReqID):
        heartbtmsg = fix.message.HeartBeatMessage(
            {**self.header_fill, 34: str(self.seq(no_raise=True)).encode(),
             112: testReqID})
        self.send_msg(bytes(heartbtmsg), log_level=logging.DEBUG)
