
import os
import re
import uuid
import psycopg2
import argparse
import xml.etree.cElementTree as ET

def listCvatKey(rootTag,cvat_key):
    for child in rootTag:
        for key in cvat_key:
            if key[4:] == child.tag:
                cvat_key[key] = child.text

def listImageKey(rootTag,image_key):
    for elem in rootTag.attrib:
        image_key[elem] = rootTag.attrib[elem]
    for child in rootTag:
        if child.tag == "attribute":
            if child.attrib["name"]:
                for key in image_key:
                    if key == child.attrib["name"]:
                        image_key[key] = child.text

def insertTable(cvat_key,image_key,xmlPath):
    cvatArr = []
    imageArr = []
    for key in cvat_key:
        cvatArr.append(cvat_key[key])
    for key in image_key:
        imageArr.append(image_key[key])
    cur.execute("""delete  from multi_point_tb where xml_path = %s and image_name = %s and insert_batch != %s""", [xmlPath, image_key['name'], str(insertBatchUuid)])
    cur.execute("""INSERT INTO multi_point_tb(annotator,camera_type,xml_id,xml_name,xml_size,xml_mode,xml_overlap,xml_bugtracker,xml_created,xml_updated,xml_start_frame,xml_stop_frame,xml_frame_filter,xml_z_order,\
    image_id,image_name,image_width,image_height,image_label,image_occluded,image_points,image_timeofday,image_weather,image_scene,first_point_type,second_point_type,third_point_pos,third_point_type,fourth_point_pos,fourth_point_type,fifth_point_type,sixth_point_type,seventh_point_type,eighth_point_type,heading,shadow,reflective,stagnant_water,special_type,line_color,line_type,ground_type,occupation,clarity,type,point,xml_path,insert_batch) 
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, [args.name, args.type] + cvatArr + imageArr + [xmlPath, str(insertBatchUuid)])
    db.commit()

def listXml(xmlPath):
    cvat_key = {
        "xml_id": None,
        "xml_name": None,
        "xml_size": None,
        "xml_mode": None,
        "xml_overlap": None,
        "xml_bugtracker": None,
        "xml_created": None,
        "xml_updated": None,
        "xml_start_frame": None,
        "xml_stop_frame": None,
        "xml_frame_filter": None,
        "xml_z_order": None
    }
    image_key = {
        "id": None,
        "name": None,
        "width":None,
        "height": None,
        "label": None,
        "occluded": None,
        "points": None,
        "timeofday": None,
        "weather": None,
        "scene": None,
        "first_point_type": None,
        "second_point_type": None,
        "third_point_pos": None,
        "third_point_type": None,
        "fourth_point_pos": None,
        "fourth_point_type": None,
        "fifth_point_type": None,
        "sixth_point_type": None,
        "seventh_point_type": None,
        "eighth_point_type": None,
        "heading":None,
        "shadow": None,
        "reflective": None,
        "stagnant_water": None,
        "special_type": None,
        "line_color": None,
        "line_type": None,
        "ground_type": None,
        "occupation": None,
        "clarity": None,
        "type": None,
        "point": None
    }
    tree = ET.parse(xmlPath)
    root = tree.getroot()
    for child in root:
        if child.tag == "meta":
            for secDeepChild in child:
                if secDeepChild.tag == "name":
                    image_key["name"] = secDeepChild.text
                if secDeepChild.tag == "width":
                    image_key["width"] = secDeepChild.text
                if secDeepChild.tag == "height":
                    image_key["height"] = secDeepChild.text
                if secDeepChild.tag == "timeofday":
                    image_key["timeofday"] = secDeepChild.text
                if secDeepChild.tag == "weather":
                    image_key["weather"] = secDeepChild.text
                if secDeepChild.tag == "scene":
                    image_key["scene"] = secDeepChild.text
                if secDeepChild.tag == "task":
                    listCvatKey(secDeepChild, cvat_key)
        if child.tag == "image":
            for elem in child.attrib:
                image_key[elem] = child.attrib[elem]
            for secDeepChild in child:
                listImageKey(secDeepChild,image_key)
                insertTable(cvat_key,image_key,xmlPath)
        if child.tag == "polygon":
            listImageKey(child, image_key)
            insertTable(cvat_key,image_key,xmlPath)

try:
    DB_HOST = 'localhost'
    DB_USER = 'postgres'
    DB_PASSWORD = '111111'
    DB_NAME = 'pydemo'
    DB_PORT = '5432'
    # DB_HOST = '192.168.3.110'
    # DB_USER = 'labeluser'
    # DB_PASSWORD = 'labelpassword'
    # DB_NAME = 'labeldb'
    currentScriptName = "script.py"
    regex = "\.xml$"
    insertBatchUuid = uuid.uuid4()

    db = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    print('数据库已建立连接...')
    cur = db.cursor()
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=None, help="......")
    parser.add_argument("--type", default=None, help="......")
    args = parser.parse_args()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS multi_point_tb(
        main_id SERIAL PRIMARY KEY,
        annotator varchar(16),
        camera_type varchar(16),
        xml_id INT,
        xml_name VARCHAR(255),
        xml_size VARCHAR(36),
        xml_mode VARCHAR(36),
        xml_overlap VARCHAR(36),
        xml_bugtracker VARCHAR(36),
        xml_created VARCHAR(255),
        xml_updated VARCHAR(255),
        xml_start_frame VARCHAR(36),
        xml_stop_frame VARCHAR(36),
        xml_frame_filter VARCHAR(36),
        xml_z_order VARCHAR(36),
        image_id INT ,
        image_name VARCHAR(255),
        image_width INT,
        image_height INT,
        image_label VARCHAR(36),
        image_occluded VARCHAR(36),
        image_points VARCHAR(512),
        image_timeofday VARCHAR(255),
        image_weather VARCHAR(255),
        image_scene VARCHAR(255),
        first_point_type VARCHAR(36),
        second_point_type VARCHAR(36),
        third_point_pos VARCHAR(36),
        third_point_type VARCHAR(36),
        fourth_point_pos VARCHAR(36),
        fourth_point_type VARCHAR(36),
        fifth_point_type VARCHAR(36),
        sixth_point_type VARCHAR(36),
        seventh_point_type VARCHAR(36),
        eighth_point_type VARCHAR(36),
        heading VARCHAR(36),
        shadow VARCHAR(36),
        reflective VARCHAR(36),
        stagnant_water VARCHAR(36),
        special_type VARCHAR(36),
        line_color VARCHAR(36),
        line_type VARCHAR(36),
        ground_type VARCHAR(36),
        occupation VARCHAR(36),
        clarity VARCHAR(36),
        type VARCHAR(36),
        point VARCHAR(255),
        xml_path VARCHAR(512),
        insert_batch VARCHAR(36)
    )"""
    cur.execute(create_table_sql)

    rootPath = os.getcwd()
    paths = os.listdir(rootPath)
    folderlist = []
    for item in paths:
        if item == currentScriptName:
            continue
        folderlist.append(item)
    for folder in folderlist:
        deepFolderPath = rootPath + '\\' + folder
        folderDeep = os.listdir(deepFolderPath)
        for fold in folderDeep:
            folderPath =  deepFolderPath +'\\' + fold
            xmls = os.listdir(folderPath)
            for xml in xmls:
                if re.search(regex, xml):
                    listXml(folderPath + '\\' + xml)

except psycopg2.Error as e:
    print('数据库连接失败:'+str(e))
    db.rollback()

db.close()
