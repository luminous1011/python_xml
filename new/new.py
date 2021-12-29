
import os
import re
import copy
import psycopg2

DB_HOST = 'localhost'
DB_USER = 'postgres'
DB_PASSWORD = '111111'
DB_NAME = 'pydemo'

try:
    # db = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
    db = psycopg2.connect( database = DB_NAME, user = DB_USER, password=DB_PASSWORD,host=DB_HOST,port="5432")
    print('数据库已建立连接...')
    cur = db.cursor()
    # cur.execute("DROP TABLE IF EXISTS CAVT_new_main_tb")
    # cur.execute("DROP TABLE IF EXISTS CAVT_new_sec_tb")
    create_main_table_sql = """create table IF NOT EXISTS CAVT_new_main_tb(
	    main_id  SERIAL  PRIMARY key    ,
	    name varchar(255)  ,
	        
	    ) """
    create_second_table_sql = """create table IF NOT EXISTS CAVT_new_sec_tb(
        sec_id SERIAL  PRIMARY key,
	    main_mid int  ,
	    label varchar(36),
	    first_points varchar(360),
	    second_points varchar(360),
	    first_point varchar(36),
	    second_point varchar(36),
	    occluded varchar(36),
	    image_id int,
	    image_name varchar(255),
	    image_width int,
	    image_height int
	    )"""
    cur.execute(create_main_table_sql)
    cur.execute(create_second_table_sql)
    path = os.getcwd()
    paths = os.listdir(path)
    path_list = []
    for item in paths:
        if item == ".idea":
            continue
        if item == "new.py":
            continue
        if item == ".git":
            continue
        path_list.append(item)

    for line in path_list:
        arr = os.listdir(path + '\\' + line)
        for j in arr:
            testArr = open(path + '\\' +line + '\\' + j, 'r').readlines()
            i = 0
            PRIMARY_TABLE_KEYS = {
                'id':'',
                'name': '',
                'size': '',
                'mode': '',
                'overlap': '',
                'bugtracker': '',
                'created': '',
                'updated': '',
                'start_frame': '',
                'stop_frame': '',
                'frame_filter': '',
                'z_order': ''
            }  # 主表key键
            SECOND_TABLE_KEYS = {
                'label': '',
                'first_points': '',
                'second_points': '',
                'first_point': '',
                'second_point': '',
                'occluded': '',
                'image_id': '',
                'image_name': '',
                'image_width': '',
                'image_height': ''
            }  # 副表key键
            polygonArr = []  # 记录一个xml文件里有几个 polygon
            index = 0  # 循环 polygonArr 数组 索引
            a=0
            enterSecTable = 0  # 控制是否读取 attribute 标签属性
            pointIndex = 0
            while a < len(testArr):
                lineContent = testArr[a].strip()
                for key in PRIMARY_TABLE_KEYS:
                    if re.match('\<' + key + '\>', lineContent):
                        value = re.findall(r">(.*)<", lineContent)[0]
                        if PRIMARY_TABLE_KEYS[key] == '':
                            PRIMARY_TABLE_KEYS[key] = value
                        break
                a+=1
            cur.execute(
                "INSERT INTO CAVT_new_main_tb(xid,name, size, mode, overlap, bugtracker,created,updated,start_frame,stop_frame,frame_filter,z_order) VALUES \
                ('" + PRIMARY_TABLE_KEYS['id'] + "','" + PRIMARY_TABLE_KEYS['name'] \
                + "','" + PRIMARY_TABLE_KEYS['size'] + "', '" + PRIMARY_TABLE_KEYS['mode'] + "','" + PRIMARY_TABLE_KEYS[
                    'overlap'] \
                + "','" + PRIMARY_TABLE_KEYS['bugtracker'] + "', '" + PRIMARY_TABLE_KEYS['created'] + "','" +
                PRIMARY_TABLE_KEYS['updated'] \
                + "','" + PRIMARY_TABLE_KEYS['start_frame'] + "', '" + PRIMARY_TABLE_KEYS['stop_frame'] + "','" +
                PRIMARY_TABLE_KEYS['frame_filter'] \
                + "','" + PRIMARY_TABLE_KEYS['z_order'] + "') returning main_id")
            insertId=cur.fetchone()[0]

            while i < len(testArr):
                lineContent = testArr[i].strip()
                # for key in PRIMARY_TABLE_KEYS:
                #     if re.match('\<' + key + '\>', lineContent):
                #         value = re.findall(r">(.*)<", lineContent)[0]
                #         if PRIMARY_TABLE_KEYS[key] == '':
                #             PRIMARY_TABLE_KEYS[key] = value
                #         break
                if re.match('<attribute', lineContent) and re.search('name', lineContent) and enterSecTable == 1:
                    value = re.findall(r">(.*?)<", lineContent)[0]
                    if pointIndex==0:
                        polygonArr[index]['first_point'] = value
                    if pointIndex==1:
                        polygonArr[index]['second_point'] = value
                if re.match('<points', lineContent)  and enterSecTable == 1:
                    polygonArr[index]['label'] = re.findall(r'label="(.*?)"', lineContent)[0]
                    polygonArr[index]['occluded'] = re.findall(r'occluded="(.*?)"', lineContent)[0]
                    value = re.findall(r'points="(.*?)"', lineContent)[0]
                    if pointIndex == 0:
                        polygonArr[index]['first_points'] = value
                    if pointIndex == 1:
                        polygonArr[index]['second_points'] = value
                if re.match('</points', lineContent):
                    pointIndex+=1
                    if pointIndex>=2:
                        pointIndex=0
                if re.match('<image', lineContent):
                    enterSecTable = 1
                    polygonArr.append(copy.deepcopy(SECOND_TABLE_KEYS))
                    polygonArr[index]['image_id'] = re.findall(r'id="(.*?)"', lineContent)[0]
                    polygonArr[index]['image_name'] = re.findall(r'name="(.*?)"', lineContent)[0]
                    polygonArr[index]['image_width'] = re.findall(r'width="(.*?)"', lineContent)[0]
                    polygonArr[index]['image_height'] = re.findall(r'height="(.*?)"', lineContent)[0]
                if re.match('</image', lineContent):
                    cur.execute(
                        "INSERT INTO CAVT_new_sec_tb(main_mid,label, first_points, second_points,first_point,second_point,occluded,image_id,image_name,image_width,image_height) VALUES \
                            ('" + str(insertId) + "', '" + polygonArr[index]['label'] + "', '" + polygonArr[index]['first_points'] + "','" + polygonArr[index]['second_points'] + "','" + polygonArr[index]['first_point'] + "','" + polygonArr[index]['second_point'] + "','" + polygonArr[index]['occluded'] + "','"\
                        + polygonArr[index]['image_id'] + "', '" + polygonArr[index]['image_name'] + "','" + polygonArr[index]['image_width'] + "','" \
                        + polygonArr[index]['image_height'] +"')")

                    enterSecTable = 0
                    index += 1
                i += 1

    # print("INSERT INTO demo_tb(name) VALUE ('"+str(PRIMARY_TABLE_KEYS['name'])+"')")
    db.commit()
except psycopg2.Error as e:
    print('数据库连接失败:'+str(e))
    db.rollback()

db.close()
