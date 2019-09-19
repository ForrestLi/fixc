import fix
import nose
from nose.tools import *
from fixclient import FixClient
import configparser
from datetime import datetime
import os

config = configparser.ConfigParser()
config.read('../../config_files/fo_fix.ini')

class TestMessage():

   def setup(self):
       self.cli   = FixClient(config=config)
       self.ordmsg   = '8=FIX.4.2^9=162^35=D^34=43^49=Client2^52=20170725-09:29:51.624^56=EMS^1=JPM^11=HKG-12-QA.00:00:00:00^21=1^38=80^40=7^44=80^54=1^55=5^59=0^60=20170725-09:29:51^115=ABC^47=A^528=A^10=140^'  
       self.amendmsg = '8=FIX.4.2^9=182^35=G^34=49^49=Client2^52=20170725-09:36:29.940^56=EMS^1=JPM^11=HKG-15-QA.00:00:00:00^21=1^38=880^40=7^44=81^41=HKG-12-QA.00:00:00:00^54=1^55=5^59=0^60=20170725-09:36:29^115=ABC^47=A^528=A^10=209^'
       print('\n-------------------------------------------TEST DESCRIPTION------------------------------------------------------------------------------------------------------------------------\n')
       print('\nThis scenario will verify that whether user can sends , creates & amends enhanced limit orders in EMS properly')
       print('        Client session will send out a enhanced limit buy day order to EMS, Expects: EMS can accept & create the order properly.')
       print('        Client session will send out an amendment msg to EMS for amending the previous enhanced limit order qty & price, Expects: EMS can accept the amendment msg & updates the order properly ')
       # add the test scenaro name in traffic log
       with open('traffic.log','a+') as traffic_log:
             traffic_log.write('\nstart executing {}\nplease see the following for the traffic log:\n'.format(os.path.basename(__file__)))
             traffic_log.write('\n')
             traffic_log.close()
   def test_amend_msg(self):
       ordmsg = self.ordmsg
       amendmsg = self.amendmsg
       cli = self.cli      
       ordmsg = fix.NewOrderMessage.parse(ordmsg.encode(), delim=b'^')
       ordmsg.update(cli.header_fill)
       amendmsg = fix.AmendOrderMessage.parse(amendmsg.encode(), delim=b'^')
       amendmsg.update(cli.header_fill)
       print('\n--------------------------------------------Test Log-------------------------------------------------------------------------------------------------------------------------------\n')
       print('\ntest run executed at {} : \n'.format(str(datetime.now())))
       with cli:
            ordmsg.delim = b'\x01'
            ordmsg.reset(seqnum=cli.seq())
            org_ordIDvalue = ordmsg[11]
            cli.send_msg(bytes(ordmsg))
            print('        Client session sent the 35=D, 11={}, 40=7, 44=80, 38=80 msg to EMS'.format(org_ordIDvalue))
            ordAckMsg_C=cli.recv_linked_ack_use_id(org_ordIDvalue)         
            assert ordAckMsg_C[150] == b'0' 
            assert ordAckMsg_C[38] == b'80'
            assert ordAckMsg_C[40] == b'7'
            assert ordAckMsg_C[44] == b'80'
            assert float(ordAckMsg_C[14])+float(ordAckMsg_C[151]) ==float(ordAckMsg_C[38])
            print ('        Order qty, price & type verficiation done for the 1st msg')
            #update the value of tag 41 to the one used in 35=D for building the order chain
            amendmsg [41] = org_ordIDvalue
            Amendmsg [38] = b'980'
            Amendmsg [44] = b'88'
            amendmsg.delim = b'\x01'
            amendmsg.reset(seqnum=cli.seq())            
            cli.send_msg(bytes(amendmsg))
            org_ordIDvalue = amendmsg [11]
            print('        Start sending the 35=G, 11={}, 40=2, 44=81, 38=880 msg to EMS'.format(org_ordIDvalue))
            amendAckMsg_A=cli.recv_linked_ack_use_id(org_ordIDvalue)
            if (amendAckMsg_A[39] != b'2'):
                assert amendAckMsg_A[150] == b'5'
                assert amendAckMsg_A[38] == b'880'
                assert amendAckMsg_A[44] == b'81'
                assert amendAckMsg_A[40] == b'7'
                assert float(amendAckMsg_A[14])+float(amendAckMsg_A[151]) ==float(amendAckMsg_A[38])
                print('        Passed verfication for price & qty update in amendment ack msg with order QTY={}, price={}, leaves QTY={}, cum QTY={}, orderType={} '.format(amendAckMsg_A[38], amendAckMsg_A[44], amendAckMsg_A[151],amendAckMsg_A[14],amendAckMsg_A[40]))

