import os
import re
import copy
import psycopg2
import argparse
import uuid
#
# DB_HOST = '192.168.3.110'
# DB_USER = 'labeluser'
# DB_PASSWORD = 'labelpassword'
# DB_NAME = 'labeldb'
DB_HOST = 'localhost'
DB_USER = 'postgres'
DB_PASSWORD = '111111'
DB_NAME = 'pydemo'

try:
    db = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port="5432")
    print('数据库已建立连接...')
    cur = db.cursor()
    insertBatchUuid = uuid.uuid4()
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="未注明标注者", help="......")
    parser.add_argument("--type", default="未注明摄像头类型", help="......")
    args = parser.parse_args()
    create_main_table_sql = """
    create table IF NOT EXISTS multipoint_main_tb (
	    main_id SERIAL PRIMARY key,
	    annotator char(16),
	    name varchar(255)   ,
	    width varchar(40) , 
	    height varchar(40) ,
	    timeofday varchar(40) ,
	    scene varchar(40) ,
	    weather varchar(40),
	    first_point_type varchar(36),
	    second_point_type varchar(36),
	    third_point_type varchar(36),
	    fourth_point_type varchar(36),
	    third_point_pos varchar(36),
	    fourth_point_pos varchar(36),
	    fifth_point_type varchar(36),
	    sixth_point_type varchar(36),
	    seventh_point_type varchar(36),
	    eighth_point_type varchar(36),
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
	    xml_id varchar(40),
        size varchar(40),
	    mode varchar(40),
	    overlap varchar(40),
	    bugtracker varchar(40),
	    created varchar(48),
	    updated varchar(48),
	    start_frame varchar(40),
	    stop_frame varchar(40),
	    frame_filter varchar(40),
	    z_order varchar(40),
	    point varchar(36),
	    image_id varchar(36),
	    filePath varchar(360) ,
	    insert_batch varchar(48),
	    camera_type varchar(36)
	    ) """
    # create_image_name_sql="""
    # create table IF NOT EXISTS image_name_tb(
    #     image_id SERIAL  PRIMARY key,
    #     main_id int,
    #     inset_batch varchar(48) 
    # )"""
    cur.execute(create_main_table_sql)

    def doInsertMainTable(PRIMARY_TABLE_KEYS,polygonArr,index,filePath):
        cur.execute("""select main_id from multipoint_main_tb where filePath = %s and name = %s and insert_batch != %s""",[filePath, PRIMARY_TABLE_KEYS['name'], str(insertBatchUuid)])
        for id in cur.fetchall():
            cur.execute(" delete from multipoint_main_tb where main_id = %s",[str(id[0])])
        cur.execute(
            "INSERT INTO multipoint_main_tb(name, width, height, weather, timeofday, scene, first_point_type, second_point_type, third_point_type, fourth_point_type, fifth_point_type, sixth_point_type, seventh_point_type, eighth_point_type,\
            type, clarity, occupation, ground_type, line_type, line_color, special_type, stagnant_water, reflective, shadow, is_overlap, doubleline, heading, \
            label, points, occluded,third_point_pos, fourth_point_pos,xml_id,size, mode, overlap, bugtracker,created,updated,start_frame,stop_frame,frame_filter,z_order,point,image_id,annotator,filePath,insert_batch,camera_type) VALUES \
            ('" + PRIMARY_TABLE_KEYS['name'] + "','" + PRIMARY_TABLE_KEYS['width'] + "','" + PRIMARY_TABLE_KEYS[
                'height'] \
            + "','" + PRIMARY_TABLE_KEYS['weather'] + "', '" + PRIMARY_TABLE_KEYS['timeofday'] + "','" +
            PRIMARY_TABLE_KEYS['scene']  + "','" + polygonArr[index]['first_point_type'] + "','" +
            polygonArr[index]['second_point_type'] + "','" + polygonArr[index]['third_point_type'] + "','" \
            + polygonArr[index]['fourth_point_type'] + "', '" + polygonArr[index][
                'fifth_point_type'] + "','" + polygonArr[index]['sixth_point_type'] + "','" \
            + polygonArr[index]['seventh_point_type'] + "', '" + polygonArr[index][
                'eighth_point_type'] + "','" + polygonArr[index]['type'] + "','" \
            + polygonArr[index]['clarity'] + "', '" + polygonArr[index]['occupation'] + "','" +
            polygonArr[index]['ground_type'] + "','" \
            + polygonArr[index]['line_type'] + "', '" + polygonArr[index]['line_color'] + "','" +
            polygonArr[index]['special_type'] + "','" \
            + polygonArr[index]['stagnant_water'] + "', '" + polygonArr[index]['reflective'] + "','" +
            polygonArr[index]['shadow'] + "','" \
            + polygonArr[index]['is_overlap'] + "', '" + polygonArr[index]['doubleline'] + "','" +
            polygonArr[index]['heading'] + "','" \
            + polygonArr[index]['label'] + "', '" + polygonArr[index]['points'] + "','" + polygonArr[index][
                'occluded'] + "','" + polygonArr[index]['third_point_pos'] + "','" \
            + polygonArr[index]['fourth_point_pos'] +"','" + PRIMARY_TABLE_KEYS['id']  \
                + "','" + PRIMARY_TABLE_KEYS['size'] + "', '" + PRIMARY_TABLE_KEYS['mode'] + "','" + PRIMARY_TABLE_KEYS[
                    'overlap'] \
                + "','" + PRIMARY_TABLE_KEYS['bugtracker'] + "', '" + PRIMARY_TABLE_KEYS['created'] + "','" +
                PRIMARY_TABLE_KEYS['updated'] \
                + "','" + PRIMARY_TABLE_KEYS['start_frame'] + "', '" + PRIMARY_TABLE_KEYS['stop_frame'] + "','" +
                PRIMARY_TABLE_KEYS['frame_filter'] \
                + "','" + PRIMARY_TABLE_KEYS['z_order'] + "', '"  + polygonArr[index]['point'] + "','" \
                        + polygonArr[index]['image_id']  + "', '" + args.name + "', '" + filePath + "', '" + str(insertBatchUuid) + "', '" + args.type + "') returning main_id ")
    def insertIntoTable(arr,path):
        for j in arr:
            filePath = path + '\\' + j
            testArr = open(filePath, 'r').readlines()
            i = 0
            a = 0
            PRIMARY_TABLE_KEYS = {
                'name': '',
                'width': '',
                'height': '',
                'timeofday': '',
                'weather': '',
                'scene': '',
                'id': '',
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
                'third_point_type': '',
                'fourth_point_type': '',
                'fifth_point_type': '',
                'sixth_point_type': '',
                'seventh_point_type': '',
                'eighth_point_type': '',
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
                'third_point_pos': '',
                'fourth_point_pos': '',
                'point': '',
                'image_id':''
            }  # 副表key键
            polygonArr = []  # 记录一个xml文件里有几个 polygon
            index = 0  # 循环 polygonArr 数组 索引
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
                a += 1

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

                if re.match('<points', lineContent)  and enterSecTable == 1:
                    polygonArr[index]['label'] = re.findall(r'label="(.*?)"', lineContent)[0]
                    polygonArr[index]['occluded'] = re.findall(r'occluded="(.*?)"', lineContent)[0]
                    value = re.findall(r'points="(.*?)"', lineContent)[0]
                    polygonArr[index]['points'] = value
                if re.match('</points', lineContent):
                    doInsertMainTable(PRIMARY_TABLE_KEYS,polygonArr,index,filePath)
                    pointIndex+=1
                    if pointIndex>=2:
                        pointIndex=0
                if re.match('<image', lineContent):
                    enterSecTable = 1
                    polygonArr.append(copy.deepcopy(SECOND_TABLE_KEYS))
                    polygonArr[index]['image_id'] = re.findall(r'id="(.*?)"', lineContent)[0]
                    PRIMARY_TABLE_KEYS['name'] = re.findall(r'name="(.*?)"', lineContent)[0]
                    PRIMARY_TABLE_KEYS['width'] = re.findall(r'width="(.*?)"', lineContent)[0]
                    PRIMARY_TABLE_KEYS['height'] = re.findall(r'height="(.*?)"', lineContent)[0]

                if re.match('</polygon', lineContent):
                    doInsertMainTable(PRIMARY_TABLE_KEYS,polygonArr,index,filePath)
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
        if item == "multipoint.py":
            continue
        if item == ".git":
            continue
        path_list.append(item)
    for folder in path_list:
        folderDeeps = os.listdir(path + '\\' + folder)
        for folderDeepItem in folderDeeps:
            xmlsPath = path + '\\' + folder + '\\' + folderDeepItem
            xmls = os.listdir(xmlsPath)
            insertIntoTable(xmls, xmlsPath)


except psycopg2.Error as e:
    print('数据库连接失败:'+str(e))
    db.rollback()

db.close()
