import requests
from bs4 import BeautifulSoup
import numpy as np
import pymysql
import csv
from requests.adapters import HTTPAdapter

class LianJiaChengJiao():
    # mydb=pymysql.connect(host='localhost',user='root',password='123456',database='django_mysql_2',charset='utf8mb4')
    # mycursor=mydb.cursor()
    district={
        'th':'tianhe',
        'yx':'yuexiu',
        'lw':'liwan',
        'hz':'haizhu',
        'py':'panyu',
        'by':'baiyun',
        'hp':'huangpu',
        'ch':'conghua',
        'zc':'zengcheng',
        'hd':'huadu',
        'ns':'nansha',
        'nh':'nanhai',
        'sd':'shunde',
        }
    def __init__(self):
        self.url='https://gz.lianjia.com/chengjiao/pg{}/'
        self.headers={
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
        }
    
    def send_request(self,url):
        resp=requests.get(url,self.headers)
        if resp.status_code==200:
            return resp

    def parse_content(self,resp):#解析内容
        html=resp.text
        bs=BeautifulSoup(html,'html.parser')
        lc=bs.find('ul',class_='listContent')
        li_lst=lc.find_all('li')
        lst=[]
        for li in li_lst:
            title=li.find('div',class_='title').text
            house_info=li.find('div',class_='houseInfo').text
            total_price=li.find('div',class_='totalPrice').text
            position_info=li.find('div',class_='positionInfo').text
            unit_price=li.find('div',class_='unitPrice').text
            lst.append((title,house_info,total_price,position_info,unit_price))
        self.write_csv(lst)
        #self.write_mysql(lst)

    def write_mysql(self,lst):#写入数据库
        pass

    def write_csv(self,lst):#写入csv文件
        with open('tmp_ljcj.csv','a') as csv_file:
            writer=csv.writer(csv_file)
            for item in lst:
                writer.writerow(item)

    def start(self):#开始
        for i in range(1,101):
            full_url=self.url.format(i)
            resp=self.send_request(full_url)
            if resp:
                self.parse_content(resp)



class BeiKeChengJiao():
    # mydb=pymysql.connect(host='localhost',user='root',password='123456',database='django_mysql_2',charset='utf8mb4')
    # mycursor=mydb.cursor()
    district={
        'th':'tianhe',
        'yx':'yuexiu',
        }
    district_sub={
        'tianhe':[
            'cencun','changxing1','chebei','dashadi','dongpu','gaotang','huajingxincheng',
            'huangcun','huijingxincheng','jinrongcheng1','linhe','longdong','longkoudong',
            'longkouxi','meihuayuan','shahe1','shataibei','shatainan','shipai1','shuiyin',
            'tangxia1','tianhegongyuan','tianhekeyunzhan','tianhenan','tianrunlu','tiyuzhongxin',
            'wushan','yantang','yuancun','yueken','yuzhu','zhihuicheng','zhujiangxinchengdong',
            'zhujiangxinchengxi','zhujiangxinchengzhong'
        ],
        'yuexiu':[
            'beijinglu','dongchuanlu','dongfengdong','donghu1','dongshankou','ershadao','gongyuanqian',
            'haizhuguangchang','huanghuagang','huanshidong','jianshelu1','jiefangbei','jiefangnan',
            'lujing','nongjiangsuo','panfu','taojin','wuyangxincheng','xiaobei','ximenkou',
            'yangji','yuexiunan'
        ]
    }

    def __init__(self):
        self.url="https://gz.ke.com/chengjiao/{}/pg{}/"
        self.header1={
            'User-Agent':"Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"
        }
        self.header2={
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
        }
        self.header3={
            'User-Agent':'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'
        }
    def send_request(self,url):
        resp=requests.get(url,timeout=7)
        if resp.status_code==200:
            return resp

    def parse_content(self,resp,i):#i为区域
        html=resp.text
        bs=BeautifulSoup(html,'html.parser')
        lc=bs.find('ul',class_='listContent')
        li_list=lc.find_all('li')
        assert len(li_list)!=0
        lst=[]
        for li in li_list:
            title=li.find('div',class_='title').text.replace('\n','')
            if type(title)=='NoneType':
                continue
            title_lst=title.split(" ")
            comm_name=title_lst[0]
            room_info=title_lst[1]
            if room_info.find('车位')!=-1 or room_info <"1室2厅":
                continue
            area_info=title_lst[2]
            house_info=li.find('div',class_='houseInfo').text.replace('\n','').replace(' ','')
            deal_date=li.find('div',class_='dealDate').text.replace('\n','').replace(' ','')
            total_price=li.find('div',class_='totalPrice').text.replace('\n','').replace(' ','')
            position_info=li.find('div',class_='positionInfo').text.replace('\n','').replace(' ','')
            unit_price=li.find('div',class_='unitPrice').text.replace('\n','').replace(' ','')
            district_info=i
            lst.append((comm_name,room_info,area_info, house_info, deal_date, total_price, position_info,unit_price,district_info))
        
        if len(lst)==0:
            return

        self.write_csv(lst)
    
    def write_csv(self,lst):
        with open('tmp_bkcj.csv','a') as csv_file:
            writer=csv.writer(csv_file)
            for item in lst:
                if item:
                    writer.writerow(item)

    # def write_mysql(self,lst):
    #     try:
    #         sql="insert into myapp_house_info_sold (comm_name, room_info, area_info, house_info, deal_date, total_price, position_info, unit_price, district_info) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #         for item in lst:
    #             self.mycursor.execute(sql,item)
    #         self.mydb.commit()
    #     except Exception as e:
    #         self.mydb.rollback()
    #         print("存储失败！",e)


    def clearDB(self):
        try:
            sql="truncate table myapp_house_info_sold"
            self.mycursor.execute(sql)
            self.mydb.commit()
        except Exception as e:
            self.mydb.rollback()
            print("清除失败",e)

    def start(self):
        for i in self.district:
            for k in self.district_sub[self.district[i]]:
                for j in range(1,101):
                    full_url=self.url.format(k,j)
                    resp=self.send_request(full_url)
                    if resp:
                        try:
                            self.parse_content(resp,i)
                        except AssertionError:
                            break
        print('Done!')



class BeikeZaiShou():#爬取贝壳在售二手房数据
    # mydb=pymysql.connect(host='localhost',user='root',password='123456',database='django_mysql_2',charset='utf8mb4')
    # mycursor=mydb.cursor()
    district={
        'th':'tianhe',
        'yx':'yuexiu',
    }

    def __init__(self):
        super().__init__()
        self.url="https://gz.ke.com/ershoufang/{}/pg{}"
        self.headers={
            'User-Agent':"Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"
        }
    def send_request(self,url):
        resp=requests.get(url,self.headers,timeout=7)
        if resp.status_code==200:
            return resp

    def parse_content(self,resp,i):#i为区域
        html=resp.text
        bs=BeautifulSoup(html,'html.parser')
        lc=bs.find('ul',class_='sellListContent')
        li_list=lc.find_all('li',class_="clear")
        if len(li_list)==0:
            return
        lst=[]
        for li in li_list:
            comm_name=li.find('div',class_="positionInfo").text.replace('\n','').replace(' ','')
            house_info=li.find('div',class_='houseInfo').text.replace('\n','').replace(' ','')
            if house_info.find('1室0厅')!=-1 or house_info.find('1室1厅')!=-1:#剔除一室两厅以下的房子
                continue
            house_info_lst=house_info.split("|")
            floor_info=None
            house_year=None
            room_info=None
            area_info=None
            direction_info=None
            if len(house_info_lst)==3:
                floor_info=house_info_lst[0]
                area_info=house_info_lst[1]
                direction_info=house_info_lst[2]
            elif len(house_info_lst)==5:
                floor_info=house_info_lst[0]
                house_year=house_info_lst[1]
                room_info=house_info_lst[2]
                area_info=house_info_lst[3]
                direction_info=house_info_lst[4]
            total_price=li.find('div',class_='totalPrice').text.replace("\n","").replace(" ","")
            unit_price=li.find('div',class_='unitPrice').text
            district_info=i
            lst.append((comm_name,floor_info,house_year,room_info,area_info,direction_info,total_price,unit_price,district_info))

        self.write_csv(lst)
        
    def write_csv(self,lst):
        with open("tmp_bkzs.csv",'a') as csv_file:
            writer=csv.writer(csv_file)
            for l in lst:
                if l:
                    writer.writerow(l)
        # print(len(lst))


    def write_mysql(self,lst):
        pass

    def clearDB(self):
        pass

    def start(self):
        for i in self.district:
            for j in range(1,101):
                full_url=self.url.format(self.district[i],j)
                resp=self.send_request(full_url)
                if resp:
                    try:
                        self.parse_content(resp,i)
                    except Exception:
                        break
        print('Done!')

def main():
    beike=BeiKeChengJiao()
    beike.start()
    



main()
