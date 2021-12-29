

import os
import re
import copy
import psycopg2

DB_HOST = 'localhost'
DB_USER = 'postgres'
DB_PASSWORD = '111111'
DB_NAME = 'pydemo'

try:
    db = psycopg2.connect( database = DB_NAME, user = DB_USER, password=DB_PASSWORD,host=DB_HOST,port="5432")
    print('数据库已建立连接...')
    cur = db.cursor()
    # cur.execute("DROP TABLE IF EXISTS CAVT_main_tb")
    # cur.execute("DROP TABLE IF EXISTS CAVT_sec_tb")
    create_main_table_sql = """create table IF NOT EXISTS CAVT_main_tb(
	    main_id SERIAL  PRIMARY key  ,
	    xid int,
	    name varchar(255) ,
	    size int,
	    mode varchar(40),
	    overlap int,
	    bugtracker varchar(40),
	    created varchar(48),
	    updated varchar(48),
	    start_frame int,
	    stop_frame int,
	    frame_filter varchar(40),
	    z_order varchar(40)	    
	    ) """
    create_second_table_sql = """create table IF NOT EXISTS CAVT_sec_tb(
        sec_id SERIAL  PRIMARY key  ,
	    main_mid int  ,
	    first_point_type varchar(36),
	    second_point_type varchar(36),
	    third_point_pos varchar(36),
	    fourth_point_pos varchar(36),
	    type varchar(36),
	    clarity varchar(36),
	    occupation varchar(36),
	    ground_type varchar(36),
	    line_type varchar(36),
	    line_color varchar(36),
	    special_type varchar(36),
	    stagnant_water varchar(36),
	    reflective varchar(36),
	    shadow varchar(36),
	    is_overlap varchar(36),
	    doubleline varchar(36),
	    heading varchar(36),
	    label varchar(36),
	    points varchar(360),
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
        if item == "CVAT.py":
            continue
        if item == "fourPoint.py":
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
                'first_point_type': '',
                'second_point_type': '',
                'third_point_pos': '',
                'fourth_point_pos': '',
                'type': '',
                'clarity': '',
                'occupation': '',
                'ground_type': '',
                'line_type': '',
                'line_color': '',
                'special_type': '',
                'stagnant_water': '',
                'reflective': '',
                'shadow': '',
                'is_overlap': '',
                'doubleline': '',
                'heading': '',
                'label': '',
                'points': '',
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
                "INSERT INTO CAVT_main_tb(xid,name, size, mode, overlap, bugtracker,created,updated,start_frame,stop_frame,frame_filter,z_order) VALUES \
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
                    polygonArr[index][re.findall(r'name="(.*?)"', lineContent)[0]] = value
                    # print(re.findall(r">(.*)<", lineContent))
                    # print(re.findall(r'name="(.*)"', lineContent))
                if re.match('<polygon', lineContent)  and enterSecTable == 1:
                    polygonArr[index]['label'] = re.findall(r'label="(.*?)"', lineContent)[0]
                    polygonArr[index]['points'] = re.findall(r'points="(.*?)"', lineContent)[0]
                    polygonArr[index]['occluded'] = re.findall(r'occluded="(.*?)"', lineContent)[0]
                if re.match('<image', lineContent):
                    enterSecTable = 1
                    polygonArr.append(copy.deepcopy(SECOND_TABLE_KEYS))
                    polygonArr[index]['image_id'] = re.findall(r'id="(.*?)"', lineContent)[0]
                    polygonArr[index]['image_name'] = re.findall(r'name="(.*?)"', lineContent)[0]
                    polygonArr[index]['image_width'] = re.findall(r'width="(.*?)"', lineContent)[0]
                    polygonArr[index]['image_height'] = re.findall(r'height="(.*?)"', lineContent)[0]
                if re.match('</image', lineContent):
                    cur.execute(
                        "INSERT INTO CAVT_sec_tb(main_mid, first_point_type, second_point_type, third_point_pos, fourth_point_pos, \
                        type, clarity, occupation, ground_type, line_type, line_color, special_type, stagnant_water, reflective, shadow, is_overlap, doubleline, heading, \
                        label, points, occluded,image_id,image_name,image_width,image_height) VALUES \
                            ('" + str(insertId) + "','" + polygonArr[index]['first_point_type'] + "','" +polygonArr[index]['second_point_type'] + "','" + polygonArr[index]['third_point_pos'] + "','" \
                        + polygonArr[index]['fourth_point_pos'] + "', '"  + polygonArr[index]['type'] + "','" \
                        + polygonArr[index]['clarity'] + "', '" + polygonArr[index]['occupation'] + "','" +polygonArr[index]['ground_type'] + "','" \
                        + polygonArr[index]['line_type'] + "', '" + polygonArr[index]['line_color'] + "','" +polygonArr[index]['special_type'] + "','" \
                        + polygonArr[index]['stagnant_water'] + "', '" + polygonArr[index]['reflective'] + "','" +polygonArr[index]['shadow'] + "','" \
                        + polygonArr[index]['is_overlap'] + "', '" + polygonArr[index]['doubleline'] + "','" +polygonArr[index]['heading'] + "','" \
                        + polygonArr[index]['label'] + "', '" + polygonArr[index]['points'] + "','" + polygonArr[index]['occluded'] + "','"\
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
