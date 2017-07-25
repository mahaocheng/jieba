# -*- coding: utf-8 -*-
from __future__ import  unicode_literals
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.analysis import RegexAnalyzer
from whoosh.analysis import Tokenizer, Token
from whoosh.qparser import QueryParser
import os, os.path
from whoosh import index
import pandas as pd
import pymssql
import jieba
import re
from pandas import Series, DataFrame
import pandas as pd
import numpy as np
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

indexdir = '/home/haohedata/test'
if not os.path.exists(indexdir):
    os.mkdir(indexdir)

#connect sql server
class MSSQL:
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
    def __GetConnect(self):
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur
    def ExecQuery(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()
        #查询完毕后必须关闭连接
        self.conn.close()
        return resList
    def ExecNonQuery(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

"""
outfile = open('/home/mahaocheng/mahaocheng/github/jieba/jieba/dict.txt' , 'a')
for line in open('/home/haohedata/test/medical_dict.txt'):
    outfile.write(line.split()[0] + ' 5' +'\n')
outfile.close()
"""
jieba.load_userdict(open("/home/mahaocheng/mahaocheng/github/jieba/jieba/dict.txt"))
#words = jieba.cut_for_search(u"糖尿病并眼部病变")
#print '/'.join(words)







class ChineseTokenizer(Tokenizer):
    """
    中文分词解析器
    """
    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=True, removestops=True, start_pos=0, start_char=0,
                 mode='', **kwargs):
        assert isinstance(value, text_type), "%r is not unicode "% value
        t = Token(positions, chars, removestops=removestops, mode=mode, **kwargs)
        list_seg = jieba.cut(value)
        for w in list_seg:
            t.original = t.text = w
            t.boost = 0.5
            if positions:
                t.pos = start_pos + value.find(w)
            if chars:
                t.startchar = start_char + value.find(w)
                t.endchar = start_char + value.find(w) + len(w)
            yield t


def chinese_analyzer():
    return ChineseTokenizer()

ms = MSSQL(host="192.168.1.231",user="sa",pwd="1",db="master")
resList = ms.ExecQuery("""
    SELECT * FROM (select
    C.[FPRN]  -- 病案号                    
    ,[FZYID]         AS  住院ID
    ,[FAGE]             AS  年龄
    ,C.[FNAME]            AS  姓名
    ,[FSEX]             AS  性别
    ,[FJOB]          AS  职业
    ,[FSTATUS]     AS  婚姻状况
    ,[FRYDEPT]     AS  入院科别名称
    ,[FCYDEPT]     AS  出院科别名称
    ,[FDAYS]     AS  实际住院
    ,[FMZZD]     AS  门诊诊断
    ,[FGMYW]     AS  药物过敏                            
    ,[FQUALITYBH]     AS  病案质量编号
    ,[FBODY]     AS  尸检
    ,[FBLOOD]     AS  血型
    ,[FZKDEPT]     AS  转科科别
    ,[FRYTJ]     AS  入院途径
    ,[FIFGMYW]     AS  过敏药物
    ,[FLYFS]            AS  离院方式    
    ,[FFBNEW]       AS  医疗付费方式
    ,[ZYHZQK]     AS  好转情况
    ,[LCLJGL] AS  临床路径管理
    ,[FZDLX]   AS 诊断类型
    ,[FJBNAME] AS 疾病名称
    ,[FRYBQ]   AS 入院病情
    ,[FOPCODE]  AS 手术编码
    ,[FOP]     AS 手术操作名称
    ,[FQIEKOUBH]  AS 切口编号
    ,[FYUHEBH]  AS 愈合编号
    ,[FDOCBH]    AS 术者编号
    ,[FMAZUI]   AS 麻醉
    ,[EMR].[dbo].[HIS_BA4].[FMZDOCT] AS 麻醉医生 --麻醉医生
    ,[EMR].[dbo].[HIS_BA4].[FSSJB]  AS 手术级别
    ,[FOPKSNAME]  AS 手术科室名称
from(SELECT  
     [EMR].[dbo].[HIS_BA1].[FPRN]     ---病案号                    
    ,[FZYID]         ---住院ID
    ,[FAGE]             ---年龄
    ,[FNAME]            ---姓名
    ,[FSEX]             ---性别
    ,[FJOB]          ---职业
    ,[FSTATUS]     ---婚姻状况
    ,[FRYDEPT]     ---入院科别名称
    ,[FCYDEPT]     ---出院科别名称
    ,[FDAYS]     ---实际住院
    ,[FMZZD]     ---门诊诊断
    ,[FGMYW]     ---药物过敏                            
    ,[FQUALITYBH]     ---病案质量编号
    ,[FBODY]     ---尸检
    ,[FBLOOD]     ---血型
    ,[FZKDEPT]     ---转科科别
    ,[FRYTJ]     ---入院途径
    ,[FIFGMYW]     ---过敏药物
    ,[FLYFS]            ---离院方式    
    ,[FFBNEW]       ---医疗付费方式 
    ,[ZYHZQK]     ---好转情况
    ,[LCLJGL] ---临床路径管理
    ,[FZDLX]   --诊断类型
    ,[FJBNAME] --疾病名称
    ,[FRYBQ]   --入院病情
  FROM [EMR].[dbo].[HIS_BA1] left join [EMR].[dbo].[HIS_BA3]
  on [EMR].[dbo].[HIS_BA1].[FPRN]=[EMR].[dbo].[HIS_BA3].[FPRN])C 
  left join [EMR].[dbo].[HIS_BA4]
  on C.[FPRN]=[EMR].[dbo].[HIS_BA4].[FPRN])A  LEFT JOIN
  (SELECT [BingAnHao] 
  ,[Bingqu]
  ,[RuYuanZhenDuan]  
  FROM [EMR].[dbo].[EMR_DOC_EMR_INFO])  B  ON A.[FPRN] =B.[BingAnHao]  """)

df_his = DataFrame(resList, columns=["FPRN","FZYID","FAGE","FNAME","FSEX","FJOB","FSTATUS","FRYDEPT","FCYDEPT","FDAYS","FMZZD","FGMYW","FQUALITYBH","FBODY","FBLOOD","FZKDEPT","FRYTJ","FIFGMYW","FLYFS","FFBNEW","ZYHZQK","LCLJGL","FZDLX","FJBNAME","FRYBQ","FOPCODE","FOP","FQIEKOUBH","FYUHEBH","FDOCBH","FMAZUI","FMZDOCT","FSSJB","FOPKSNAME","BingAnHao","Bingqu","RuYuanZhenDuan"])
df_his = df_his.replace([None],[NaN])

"""
schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)
ix = create_in("/home/haohedata/test/testwhoosh", schema)
writer = ix.writer()
writer.add_document(title=u"First document", path=u"/a",
                    content=u"This is the first document we've added!")
writer.add_document(title=u"Second document", path=u"/b",
                    content=u"The second one is even more interesting!")
writer.commit()
from whoosh.qparser import QueryParser
with ix.searcher() as searcher:
    query = QueryParser("content", ix.schema).parse("first")
    results = searcher.search(query)
    results[0]
#load data


with ix.searcher() as s:
    qp = qparser.QueryParser("title", ix.schema)
    q = qp.parse('big')
    stored_fields = s.stored_fields(1)
    print stored_fields
    # Sort search results from lowest to highest price
    results = s.search(q, sortedby="price", terms=True)
    for hit in results:
        print(hit)
    print("Matched:", hit.matched_terms())
    # Which terms from the query didn't match in this hit?
    print("Didn't match:", q.all_terms() - hit.matched_terms())   
"""
#list = jieba.cut(a[0])

def join_func(group):
    group = set(group[group.notnull()])
    return ' '.join(group)

df_dropdupli = df_his.drop_duplicates(['FPRN']).sort_values(by='FPRN')
group_names = ["FJBNAME","FRYBQ","FOPCODE","FOP","FQIEKOUBH","FYUHEBH",
               "FDOCBH","FMAZUI","FMZDOCT","FSSJB","FOPKSNAME","BingAnHao",
               "Bingqu","RuYuanZhenDuan"]
for i in group_names:
    df_dropdupli[i] = list(df_his.groupby(['FPRN'])[i].apply(join_func).sort_index())

all_fields = ['FPRN', 'FZYID', 'FAGE', 'FNAME', 'FSEX', 
'FJOB', 'FSTATUS', 'FRYDEPT', 'FCYDEPT', 'FDAYS',
 'FMZZD', 'FGMYW', 'FQUALITYBH', 'FBODY', 'FBLOOD', 
 'FZKDEPT', 'FRYTJ', 'FIFGMYW', 'FLYFS', 'FFBNEW', 
 'ZYHZQK', 'LCLJGL', 'FZDLX', 'FJBNAME', 'FRYBQ', 
 'FOPCODE', 'FOP', 'FQIEKOUBH', 'FYUHEBH', 'FDOCBH',
  'FMAZUI', 'FMZDOCT', 'FSSJB', 'FOPKSNAME', 'BingAnHao', 
  'Bingqu', 'RuYuanZhenDuan']

#df_dropdupli = df_dropdupli.replace([''],[None])
analyzer = chinese_analyzer()
schema = Schema(FPRN=ID(stored=True),
                Bingqu=TEXT(stored=True), 
                FJBNAME=TEXT(stored=True, analyzer=analyzer))


ix = create_in(indexdir, schema)
writer = ix.writer()
for i in range(len(df_dropdupli)):
    FPRN = list(df_dropdupli['FPRN'])[i].decode('utf8')
    Bingqu=list(df_dropdupli['Bingqu'])[i].decode('utf8')
    FJBNAME=list(df_dropdupli['FJBNAME'])[i].decode('utf8')
    writer.add_document(FPRN=FPRN, Bingqu=Bingqu, FJBNAME=FJBNAME)


writer.commit()
while True:
    user_input = raw_input("请输入病案号或疾病名称> ")
    if user_input in list(df_dropdupli['FPRN']):
        myquery = str(df_dropdupli[df_dropdupli['FPRN']==user_input]['FJBNAME']).decode('utf8')
        print myquery
    else:
        myquery = user_input.decode('utf8')
        print myquery
    #myquery = str(df_dropdupli[df_dropdupli['FPRN']=="001903"]['FJBNAME']).decode('utf8')
    #myquery = u"腔隙性脑梗塞"
    
    print myquery
    from whoosh import qparser
    with ix.searcher() as searcher:
        query = qparser.QueryParser("FJBNAME", ix.schema ,group=qparser.OrGroup).parse(myquery)
        results = searcher.search(query)
        FPRN_list = []
        for hit in results:
            print hit['FJBNAME'], hit['Bingqu']
            print hit['FPRN']
            print hit.score
            print hit.highlights("FJBNAME", top=20)
            FPRN_list.append(hit['FPRN'])
            print '.'*100
        print 'FPRN_list',FPRN_list

