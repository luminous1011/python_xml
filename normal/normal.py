import os
import re
import copy
import psycopg2

DB_HOST = 'localhost'
DB_USER = 'postgres'
DB_PASSWORD = '111111'
DB_NAME = 'pydemo'

try:
    db = psycopg2.connect(database = DB_NAME, user = DB_USER, password=DB_PASSWORD,host=DB_HOST,port="5432")
    print('数据库已建立连接...')
    cur = db.cursor()
    create_main_table_sql = """create table IF NOT EXISTS four_point_main_tb(
	    main_id SERIAL  PRIMARY key  ,
	    name varchar(255)   ,
	    width varchar(40) , 
	    height varchar(40) ,
	    timeofday varchar(40) ,
	    scene varchar(40) ,
	    weather varchar(40)
	    ) """
    create_second_table_sql = """create table IF NOT EXISTS four_point_sec_tb(
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
	    occluded varchar(36)
	    )"""
    cur.execute(create_main_table_sql)
    cur.execute(create_second_table_sql)

    def doInsertSecTable(polygonArr,insertId,index):
        cur.execute(
            "INSERT INTO four_point_sec_tb(main_mid, first_point_type, second_point_type, third_point_pos, fourth_point_pos, \
            type, clarity, occupation, ground_type, line_type, line_color, special_type, stagnant_water, reflective, shadow, is_overlap, doubleline, heading, \
            label, points, occluded) VALUES \
                ('" + str(insertId) + "','" + polygonArr[index]['first_point_type'] + "','" +
            polygonArr[index]['second_point_type'] + "','" + polygonArr[index]['third_point_pos'] + "','" \
            + polygonArr[index]['fourth_point_pos'] + "', '" + polygonArr[index]['type'] + "','" \
            + polygonArr[index]['clarity'] + "', '" + polygonArr[index]['occupation'] + "','" +
            polygonArr[index]['ground_type'] + "','" \
            + polygonArr[index]['line_type'] + "', '" + polygonArr[index]['line_color'] + "','" +
            polygonArr[index]['special_type'] + "','" \
            + polygonArr[index]['stagnant_water'] + "', '" + polygonArr[index]['reflective'] + "','" +
            polygonArr[index]['shadow'] + "','" \
            + polygonArr[index]['is_overlap'] + "', '" + polygonArr[index]['doubleline'] + "','" +
            polygonArr[index]['heading'] + "','" \
            + polygonArr[index]['label'] + "', '" + polygonArr[index]['points'] + "','" + polygonArr[index][
                'occluded'] + \
            "')")
    def doInsertMainTable(PRIMARY_TABLE_KEYS):
        cur.execute(
            "INSERT INTO four_point_main_tb(name, width, height, weather, timeofday, scene) VALUES \
            ('" + PRIMARY_TABLE_KEYS['name'] + "','" + PRIMARY_TABLE_KEYS['width'] + "','" + PRIMARY_TABLE_KEYS[
                'height'] \
            + "','" + PRIMARY_TABLE_KEYS['weather'] + "', '" + PRIMARY_TABLE_KEYS['timeofday'] + "','" +
            PRIMARY_TABLE_KEYS['scene'] + "') returning main_id")
    def insertIntoTable(arr,path):
        # arr = os.listdir(path)
        for j in arr:
            testArr = open(path + '\\' + j, 'r').readlines()
            i = 0
            PRIMARY_TABLE_KEYS = {
                'name': '',
                'width': '',
                'height': '',
                'timeofday': '',
                'weather': '',
                'scene': ''
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
                'occluded': ''
            }  # 副表key键
            polygonArr = []  # 记录一个xml文件里有几个 polygon
            index = 0  # 循环 polygonArr 数组 索引
            a = 0
            enterSecTable = 0  # 控制是否读取 attribute 标签属性
            while a < len(testArr):
                lineContent = testArr[a].strip()
                for key in PRIMARY_TABLE_KEYS:
                    if re.match('\<' + key + '\>', lineContent):
                        value = re.findall(r">(.*)<", lineContent)[0]
                        if PRIMARY_TABLE_KEYS[key] == '':
                            PRIMARY_TABLE_KEYS[key] = value
                        break
                a += 1
            doInsertMainTable(PRIMARY_TABLE_KEYS)
            insertId = cur.fetchone()[0]
            while i < len(testArr):
                lineContent = testArr[i].strip()

                if re.match('<attribute', lineContent) and re.search('name', lineContent) and enterSecTable == 1:
                    value = re.findall(r">(.*?)<", lineContent)[0]
                    polygonArr[index][re.findall(r'name="(.*?)"', lineContent)[0]] = value

                if re.match('<polygon', lineContent):
                    enterSecTable = 1
                    polygonArr.append(copy.deepcopy(SECOND_TABLE_KEYS))
                    polygonArr[index]['label'] = re.findall(r'label="(.*?)"', lineContent)[0]
                    polygonArr[index]['points'] = re.findall(r'points="(.*?)"', lineContent)[0]
                    polygonArr[index]['occluded'] = re.findall(r'occluded="(.*?)"', lineContent)[0]

                if re.match('</polygon', lineContent):
                    doInsertSecTable(polygonArr, insertId, index)
                    enterSecTable = 0
                    index += 1
                i += 1
        db.commit()


    path = os.getcwd()
    paths = os.listdir(path)
    path_list = []
    for item in paths:
        if item == ".idea":
            continue
        if item == "normal.py":
            continue
        if item == ".git":
            continue
        path_list.append(item)
    for folder in path_list:
        folderDeeps = os.listdir(path + '\\' + folder)
        for folderDeepItem in folderDeeps:
            xmlsPath = path + '\\' + folder + '\\' + folderDeepItem
            xmls = os.listdir(xmlsPath)
            insertIntoTable(xmls,xmlsPath)
except psycopg2.Error as e:
    print('数据库连接失败:'+str(e))
    db.rollback()

db.close()
