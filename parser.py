# -*- coding: utf-8 -*-
"""
Created on Sun Sep 04 18:43:03 2016

@author: Adarsh Shetty

This is a python parser that parsers evey message type from the NASDAQ ITCH 5.0 binary data feed.
The messages parsed are stored inside a csv file with each row of the csv corresponding to every message byte.
"""

import gzip
import struct
import csv



def read_binary_file(size):
    read = bin_data.read(size);
    return read;

def system_event_message(msg):
    msg_type = 'S';
    temp = struct.unpack('>HH6sc',msg);
    new_msg = struct.pack('>sHH2s6sc',msg_type,temp[0],temp[1],'x\00x\00',temp[2],temp[3]);
    val = struct.unpack('>cHHQc',new_msg);
    val = list(val);
    return val;

def stock_directory_message(msg):
    msg_type = 'R';
    temp = struct.unpack('>4s6s10cI9cIc',msg);
    new_msg = struct.pack('>s4s2s6s10sI9sIs',msg_type,temp[0],'\x00\x00',temp[1],''.join(list(temp[2:12])),temp[12],''.join(list(temp[13:22])),temp[22],temp[23]);
    val = struct.unpack('>sHHQ8sssIss2ssssssIs',new_msg);
    val = list(val);
    return val;
	
def stock_trading_action(msg):
    msg_type = 'H';
    temp = struct.unpack('>4s6s14c',msg);
    new_msg = struct.pack('>s4s2s6s14s',msg_type,temp[0],'\x00\x00',temp[1],''.join(list(temp[2:16])));
    val = struct.unpack('>sHHQ8sss4s',new_msg);
    val = list(val);
    return val;
	
def short_sale_price_test(msg):
    msg_type = 'Y';
    temp = struct.unpack('>4s6s9c',msg);
    new_msg = struct.pack('>s4s2s6s9s',msg_type,temp[0],'\x00\x00',temp[1],''.join(list(temp[2:11])));
    val = struct.unpack('>sHHQ8ss',new_msg);
    val = list(val);
    return val;
	
def market_participation_position(msg):
    msg_type = 'L';
    temp = struct.unpack('>4s6s15c',msg);
    new_msg = struct.pack('>s4s2s6s15s',msg_type,temp[0],'\x00\x00',temp[1],''.join(list(temp[2:17])));
    val = struct.unpack('>sHHQ4s8ssss',new_msg);
    val = list(val);
    return val;
	
def mwcb_decline_level_message(msg):
    msg_type = 'V';
    temp = struct.unpack('>4s6s24s',msg);
    new_msg = struct.pack('>s4s2s6s24s',msg_type,temp[0],'\x00\x00',temp[1],temp[2]);
    val = struct.unpack('>sHHQQQQ',new_msg);
    val = list(val);
    val[4:7] = map(float,val[4:7]);
    val[4:7] = map(lambda x:x/(pow(10,8)),val[4:7]);
    return val;
	
def mwcb_status_message(msg):
    msg_type = 'W';
    temp = struct.unpack('>4s6sc',msg);
    new_msg = struct.pack('>s4s2s6ss',msg_type,temp[0],'\x00\x00',temp[1],temp[2]);
    val = struct.unpack('>sHHQs',new_msg);
    val = list(val);
    return val;
	
def ipo_quoting_period_update(msg):
    msg_type = 'K';
    temp = struct.unpack('>4s6s8cIcI',msg);
    new_msg = struct.pack('>s4s2s6s8sIsI',msg_type,temp[0],'\x00\x00',temp[1],''.join(list(temp[2:10])),temp[10],temp[11],temp[12]);
    val = struct.unpack('>sHHQ8sIsI',new_msg);
    val = list(val);
    val[7] = float(val[7]);
    val[7] = val[7]/10000;
    return val;
	
def add_order_no_mpid(msg):
    msg_type = 'A';
    temp = struct.unpack('>4s6sQcI8cI',msg);
    new_msg = struct.pack('>s4s2s6sQsI8sI',msg_type,temp[0],'\x00\x00',temp[1],temp[2],temp[3],temp[4],''.join(list(temp[5:13])),temp[13]);
    val = struct.unpack('>sHHQQsI8sI',new_msg);
    val = list(val);
    val[8] = float(val[8]);
    val[8] = val[8]/10000;
    return val;
	
def add_order_with_mpid(msg):
    msg_type = 'F';
    temp = struct.unpack('>4s6sQcI8cI4c',msg);
    new_msg = struct.pack('>s4s2s6sQsI8sI4s',msg_type,temp[0],'\x00\x00',temp[1],temp[2],temp[3],temp[4],''.join(list(temp[5:13])),temp[13],''.join(list(temp[14:18])));
    val = struct.unpack('>sHHQQsI8sI4s',new_msg);
    val = list(val);
    val[8] = float(val[8]);
    val[8] = val[8]/10000; 
    return val;
	
def order_executed_message(msg):
    msg_type = 'E';
    temp = struct.unpack('>4s6sQIQ',msg);
    new_msg = struct.pack('>s4s2s6sQIQ',msg_type,temp[0],'\x00\x00',temp[1],temp[2],temp[3],temp[4]);
    val = struct.unpack('>sHHQQIQ',new_msg);
    val = list(val);
    return val;
	
def order_executed_price_message(msg):
    msg_type = 'C';
    temp = struct.unpack('>4s6sQIQcI',msg);
    new_msg = struct.pack('>s4s2s6sQIQsI',msg_type,temp[0],'\x00\x00',temp[1],temp[2],temp[3],temp[4],temp[5],temp[6]);
    val = struct.unpack('>sHHQQIQsI',new_msg);
    val = list(val);
    val[8] = float(val[8]);
    val[8] = val[8]/10000; 
    return val;
	
def order_cancel_message(msg):
    msg_type = 'X';
    temp = struct.unpack('>4s6sQI',msg);
    new_msg = struct.pack('>s4s2s6sQI',msg_type,temp[0],'\x00\x00',temp[1],temp[2],temp[3]);
    val = struct.unpack('>sHHQQI',new_msg);
    val = list(val);
    return val;
	
def order_delete_message(msg):
    msg_type = 'D';
    temp = struct.unpack('>4s6sQ',msg);
    new_msg = struct.pack('>s4s2s6sQ',msg_type,temp[0],'\x00\x00',temp[1],temp[2]);
    val = struct.unpack('>sHHQQ',new_msg);
    val = list(val);
    return val;
	
def order_replace_message(msg):
    msg_type = 'U';
    temp = struct.unpack('>4s6sQQII',msg);
    new_msg = struct.pack('>s4s2s6sQQII',msg_type,temp[0],'\x00\x00',temp[1],temp[2],temp[3],temp[4],temp[5]);
    val = struct.unpack('>sHHQQQII',new_msg);
    val = list(val);
    val[7] = float(val[7]);
    val[7] = val[7]/10000;
    return val;
	
def trade_message(msg):
    msg_type = 'P';
    temp = struct.unpack('>4s6sQcI8cIQ',msg);
    new_msg = struct.pack('>s4s2s6sQsI8sIQ',msg_type,temp[0],'\x00\x00',temp[1],temp[2],temp[3],temp[4],''.join(list(temp[5:13])),temp[13],temp[14]);
    val = struct.unpack('>sHHQQsI8sIQ',new_msg);
    val = list(val);
    val[8] = float(val[8]);
    val[8] = val[8]/10000;
    return val;
	
def cross_trade_message(msg):
    msg_type = 'Q';
    temp = struct.unpack('>4s6sQ8cIQc',msg);
    new_msg = struct.pack('>s4s2s6sQ8sIQs',msg_type,temp[0],'\x00\x00',temp[1],temp[2],''.join(list(temp[3:11])),temp[11],temp[12],temp[13]);
    val = struct.unpack('>sHHQQ8sIQs',new_msg);
    val = list(val);
    val[6] = float(val[6]);
    val[6] = val[6]/10000;
    return val;
	
def broken_trade_execution_message(msg):
    msg_type = 'B';
    temp = struct.unpack('>4s6sQ',msg);
    new_msg = struct.pack('>s4s2s6sQ',msg_type,temp[0],'\x00\x00',temp[1],temp[2]);
    val = struct.unpack('>sHHQQ',new_msg);
    val = list(val);
    return val;
	
def net_order_imbalance_message(msg):
    msg_type = 'I';
    temp = struct.unpack('>4s6s16s9c12s2c',msg);
    new_msg = struct.pack('>s4s2s6s16s9s12s2s',msg_type,temp[0],'\x00\x00',temp[1],temp[2],''.join(list(temp[3:12])),temp[12],''.join(list(temp[13:15])));
    val = struct.unpack('>sHHQQQs8sIIIss',new_msg);
    val = list(val);
    val[8:11] = map(float,val[8:11]);
    val[8:11] = map(lambda x:x/10000,val[8:11]);
    return val;
	
def retail_price_improvement_indicator(msg):
    msg_type = 'N';
    temp = struct.unpack('>4s6s9c',msg);
    new_msg = struct.pack('>s4s2s6s9s',msg_type,temp[0],'\x00\x00',temp[1],''.join(list(temp[2:11])));
    val = struct.unpack('>sHHQ8ss',new_msg);
    val = list(val);
    return val;

bin_data = gzip.open('PATH\\07292016.NASDAQ_ITCH50.gz','rb');
out_file = open('parsed_data.csv','wb');
writer = csv.writer(out_file);
message_type = bin_data.read(1);
while message_type:
    if message_type == "S":
        message = read_binary_file(11);
        parsed_data = system_event_message(message);
        writer.writerow(parsed_data)
    elif message_type == "R":
        message = read_binary_file(38);
        parsed_data = stock_directory_message(message);
        writer.writerow(parsed_data)
    elif message_type == "H":
        message = read_binary_file(24);
        parsed_data = stock_trading_action(message);
        writer.writerow(parsed_data)
    elif message_type == "Y":
        message = read_binary_file(19);
        parsed_data = short_sale_price_test(message);
        writer.writerow(parsed_data)
    elif message_type == "L":
        message = read_binary_file(25);
        parsed_data = market_participation_position(message);
        writer.writerow(parsed_data)
    elif message_type == "V":
        message = read_binary_file(34);
        parsed_data = mwcb_decline_level_message(message);
        writer.writerow(parsed_data)
    elif message_type == "W":
        message = read_binary_file(11);
        parsed_data = mwcb_status_message(message);
        writer.writerow(parsed_data)
    elif message_type == "K":
        message = read_binary_file(27);
        parsed_data = ipo_quoting_period_update(message);
        writer.writerow(parsed_data)
    elif message_type == "A":
        message = read_binary_file(35);
        parsed_data = add_order_no_mpid(message);
        writer.writerow(parsed_data)
    elif message_type == "F":
        message = read_binary_file(39);
        parsed_data = add_order_with_mpid(message);
        writer.writerow(parsed_data)
    elif message_type == "E":
        message = read_binary_file(30);
        parsed_data = order_executed_message(message);
        writer.writerow(parsed_data)
    elif message_type == "C":
        message = read_binary_file(35);
        parsed_data = order_executed_price_message(message);
        writer.writerow(parsed_data)
    elif message_type == "X":
        message = read_binary_file(22);
        parsed_data = order_cancel_message(message);
        writer.writerow(parsed_data)
    elif message_type == "D":
        message = read_binary_file(18);
        parsed_data = order_delete_message(message);
        writer.writerow(parsed_data)
    elif message_type == "U":
        message = read_binary_file(34);
        parsed_data = order_replace_message(message);
        writer.writerow(parsed_data)
    elif message_type == "P":
        message = read_binary_file(43);
        parsed_data = trade_message(message);
        writer.writerow(parsed_data)
    elif message_type == "Q":
        message = read_binary_file(39);
        parsed_data = cross_trade_message(message);
        writer.writerow(parsed_data)
    elif message_type == "B":
        message = read_binary_file(18);
        parsed_data = broken_trade_execution_message(message);
        writer.writerow(parsed_data)
    elif message_type == "I":
        message = read_binary_file(49);
        parsed_data = net_order_imbalance_message(message);
        writer.writerow(parsed_data)
    elif message_type == "N":
        message = read_binary_file(19);
        parsed_data = retail_price_improvement_indicator(message);
        writer.writerow(parsed_data)
    message_type = bin_data.read(1);
	
bin_data.close();
out_file.close();
