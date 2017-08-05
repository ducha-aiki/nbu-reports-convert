#!/usr/bin/python2 -utt
# -*- coding: utf-8 -*-
import pandas as pd
import pandas_profiling
import numpy as np
import csv
import os
from collections import defaultdict
from csv import reader
#Read_per_quater_data
data_dir = 'data/additional_NBU/out_csvs/'

quater_files = os.listdir(data_dir)
def get_date_from_fname(fname):
    eight_digits = fname.replace('_','')
    eight_digits = eight_digits.replace(' ','')
    day = eight_digits[0:2]
    month = eight_digits[2:4]
    year = eight_digits[4:8]
    return day,month,year
partial_columns_to_merge_with_prevs_list = [
    u'у тому числі',
    u'ризику за такими операціями',
]
exact_column_names_to_merge_with_prevs = [
    u'в іноземній валюті'
]
hardcode_fix_column = u'Усього активів__у тому числі в іноземній валюті'
sheets_idx  = {u'Прибутки і збитки': 0,
               u'Показники про фінансові результати банків': 0,
               u'Показники про фінансові результати': 0,
u'Зобов': 1,
u'Активи' : 2,
u'Власний': 3 ,
u'Окремі': 4}
def read_nbu_csv(fname):
    with open(fname, 'rb') as f:
        lines = f.readlines()
        out_lines = []
        start_idx = 0
        found = False
        for l in lines:
            if u'№' in l.decode('utf-8'):
                found = True
                break
            start_idx+=1
        if not found:
            return []
        data_start_idx = start_idx
        for l in lines[start_idx:]:
            for data in reader([l]):
                break
            if data[0].strip() != '1':
                data_start_idx+=1
                continue
            else:
                break
        for column_names in reader([lines[start_idx]]):
            break
        new_column_names = []
        last_non_empty_name  = ''
        for cn in column_names:
            merge_with_prev = False
            if cn.strip() != '':
                for substr in partial_columns_to_merge_with_prevs_list:
                    if substr in cn.decode('utf-8'):
                        merge_with_prev = True
                for str1 in exact_column_names_to_merge_with_prevs:
                    if cn.decode('utf-8').strip() == str1:
                        merge_with_prev = True
                if merge_with_prev:
                    last_non_empty_name = last_non_empty_name + '__' + cn
                else:
                    last_non_empty_name = cn
            else:
                if last_non_empty_name.decode('utf-8') == u'№ з/гр':
                    last_non_empty_name = 'NKB'
            new_column_names.append(last_non_empty_name)
        for l in lines[start_idx + 1 :data_start_idx]:
            if u'Група' in l.decode('utf-8'):
                continue
            for line_split in reader([l]):
                break
            for add_idx in range(len(line_split)):
                merge_with_prev = False
                if line_split[add_idx].strip() != '':
                    for substr in partial_columns_to_merge_with_prevs_list:
                        if substr in line_split[add_idx].strip().decode('utf-8'):
                            merge_with_prev = True
                    for str1 in exact_column_names_to_merge_with_prevs:
                        if line_split[add_idx].strip().decode('utf-8') == str1:
                            merge_with_prev = True
                    if merge_with_prev:
                        new_column_names[add_idx] = new_column_names[add_idx-1] + '__' + line_split[add_idx]
                    else:
                        new_column_names[add_idx] = new_column_names[add_idx] + '_' + line_split[add_idx]
        out_cols_set = set()
        iii = 0
        max_idx = None
        repeats = False
        for cn in new_column_names:
            if cn.decode('utf-8') == hardcode_fix_column:
                max_idx = iii
                break
            if cn not in out_cols_set:
                out_cols_set.add(cn)
            else:
                repeats = True
                if cn.decode('utf-8') != u'Кредити та заборгованість клієнтів_резерви під знецінення кредитів та заборгованості клієнтів':
                    print 'Warning!!!!'
                    print iii,cn, 'is already here'
                else:
                    HARDCODE_FIX = True
                rep_iii = iii
            iii+=1
        if max_idx is not None:
            new_column_names = new_column_names[:max_idx]
        if repeats and HARDCODE_FIX:
            new_column_names[rep_iii] = u'Кредити та заборгованість клієнтів_фізичних осіб_резерви під знецінення кредитів та заборгованості клієнтів'
            new_column_names[rep_iii - 3] = u'Кредити та заборгованість клієнтів_юридичних осіб_резерви під знецінення кредитів та заборгованості клієнтів'
            #print '**********8'
            #for ppp in new_column_names[10:18]:
            #    print ppp
        #if len(new_column_names) != len(set(new_column_names)):
        #    print 'Attention!'
        out_lines.append(new_column_names)          
        #print len(column_names),len(new_column_names), len(lines[start_idx].split(','))
        ############
        for l in lines[data_start_idx:]:
            for data in reader([l]):
                break
            if len(data[0]) == 0:
                continue
            if max_idx is not None:
                data = data[:max_idx]
            out_lines.append(data)
            #print len(data)
    return out_lines

quarter_dict_data = defaultdict(dict)
for fname in quater_files:
    #if '01072014.xls.csv.1' not in fname:
    #    continue
    #if fname[-1] != '0':
    #    continue
    print fname
    full_fname = data_dir + fname
    sheet_idx = fname.strip().split('.')[-1]
    if not os.path.isfile(full_fname):
        continue
    day,month,year = get_date_from_fname(fname)
    if year not in quarter_dict_data:
        quarter_dict_data[year] = {}
    if month not in quarter_dict_data[year]:
        quarter_dict_data[year][month] = {}
    with open(data_dir + fname, 'rb') as f:
        lines = f.readlines()
        found = False
        if len(lines) < 10:
            continue
        for k, v in sheets_idx.iteritems():
            if (k in lines[0].decode('utf-8')) or  (k in lines[1].decode('utf-8')):
                sh_idx = v
                found = True
                break
        if not found:
            if '<' in lines[1].decode('utf-8'):
                continue
            print "AAAAAAAAAAAAAA", fname
    quarter_dict_data[year][month][sh_idx] = read_nbu_csv(data_dir + fname)
    #break
df_dict = {}
for k,v in quarter_dict_data.iteritems():
    if k not in df_dict:
        df_dict[k] = {}
    print k
    for kk,vv in v.iteritems():
        print kk
        if kk not in df_dict[k]:
            df_dict[k][kk] = {}
        for kkk,vvv in vv.iteritems():
            print kkk
            try:
                df_dict[k][kk][kkk] = pd.DataFrame(vvv[1:], columns=vvv[0])
            except:
                print k,kk,kkk, 'fails'
                df_dict[k][kk][kkk] = None
    print '*****'
#Merge reports

for k,v in df_dict.iteritems():
    print k
    for kk,vv in v.iteritems():
        print kk
        for kkk,vvv in vv.iteritems():
            try:
                vvv['YEAR'] =  k
                vvv['MONTH'] = kk
                vvv['QUARTER'] = kkk
            except:
                print k,kk,kkk, 'fail'
                pass
            #print vvv
            #break
sheets_dict_unmerged = defaultdict(list)

for k,v in df_dict.iteritems():
    print k
    for kk,vv in v.iteritems():
        print kk
        for kkk,vvv in vv.iteritems():
            print kkk
            if vvv is not None:
                if len(vvv) > 5:
                    #print vvv.head()
                    sheets_dict_unmerged[kkk].append(vvv)
sheets_dict_merged = {}
for sn, list_of_dfs in sheets_dict_unmerged.iteritems():
    print sn, len(list_of_dfs)
    sheets_dict_merged[sn] = pd.concat(list_of_dfs, ignore_index = True)
for k,v in sheets_dict_merged.iteritems():
    v.to_csv('data/combined/export_' + str(k) + '.csv', encoding = 'utf-8')