
# -*- coding: utf-8 -*-
from utils import connect_to_tm1
from utils import connect_to_postgresql
from TM1py.Objects import Dimension,Hierarchy
import argparse

def create_tm1_dimension_from_database(conn:'Qsql Connection',tm1:'TM1 Connection',dim_name:str,table_name:str,str_columns:str):
    #获取数据
    cursor = conn.cursor()
    # table_name = 'channel_dimension'
    # str_columns = '渠道'
    table_name = table_name
    str_columns = str_columns
    cursor.execute(f"SELECT {str_columns} FROM {table_name}")
    data = cursor.fetchall()

    # 判断维度是否存在
    if not tm1.dimensions.exists(dim_name):
        new_dimension = Dimension(dim_name)
        new_hierarchy = Hierarchy(dim_name, dim_name)
        new_dimension.add_hierarchy(new_hierarchy)
        tm1.dimensions.create(new_dimension)
    # Test01
    #tm1.processes.execute_ti_code(["DimensionElementInsertDirect('tm1py_test_lock_channel', '', 'Element02', 'N');"])
    #tm1.processes.execute_ti_code(["CellPutS('aaa', 'tm1py_lock_test', '2016', 'M01', 'Element02', 'Comment');"])
    # Test02
    #success,status,error_log = tm1.processes.execute_with_return(process_name='Master Data Update')
    # Test03
    # if tm1.subsets.exists('subset_test','tm1py_test_lock_channel','tm1py_test_lock_channel'):
    #     s=tm1.subsets.get_element_names('tm1py_test_lock_channel','tm1py_test_lock_channel','subset_test')
    #     print(s)
    # Get the hierarchy
    hierarchy = tm1.dimensions.hierarchies.get(dim_name, dim_name)
    # Get Existing Edges & # Get Existing Element
    existing_edges_set=set()
    existing_elements_set = set()
    hierarchy_data_edges_set=set()
    hierarchy_data_elements_set = set()

    # hierarchy_data from database
    if len(data)>0:
        hierarchy_data = []
        for tup in data:
            # 维表多列情形,才会有edge关系
            if len(tup)>1:
                for i in range(len(tup)-1):
                    if tup[i] !='' and tup[i+1] !='' and tup[i] != None and tup[i+1] != None:
                        hierarchy_data.append((tup[i+1],tup[i]))
            # 收集每行的element
            for ele in tup:
                    if ele != '' and ele != None:
                        hierarchy_data_elements_set.add(ele)
        hierarchy_data_edges_set = set(hierarchy_data)


    # unwind elements which need to
    hierarchy.remove_all_edges()
    # add elements which need to
    for ele in hierarchy_data_elements_set:
        try:
            hierarchy.add_element(ele, 'Numeric')
        except ValueError:
            print(f"Warning: Could not add element {ele}")
            continue

    # Update edges which need to
    for edge in hierarchy_data_edges_set:
        hierarchy.add_edge(parent=edge[0], component=edge[1], weight=1)
        #raise ValueError("Error: Could not add edge")




    # Update the hierarchy
    tm1.dimensions.hierarchies.update(hierarchy)
    # test 04
    # para = {'pSrcDim':'Business Unit','pTgtDim':'Business Unit Clone'}
    # success, status, error_log = tm1.processes.execute_with_return(process_name='}bedrock.dim.clone',**para)
if __name__ == '__main__':
    # arg
    try:
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('--dim_name', required=True, help='TM1 dimension name')
        arg_parser.add_argument('--table_name', required=True, help='Data table name')
        arg_parser.add_argument('--str_columns', required=True, help='String columns in data table')
        arg_parser.add_argument('--success_file', required=True, help='')
        args = arg_parser.parse_args()
        with connect_to_postgresql(username='postgres', password='1234') as conn:
            with connect_to_tm1(username='neil',password='123') as tm1:
                create_tm1_dimension_from_database(conn,tm1,args.dim_name,args.table_name,args.str_columns)
                if args.success_file:
                    with open(args.success_file, 'w') as f:
                        f.write('success')
    except Exception as e:
        raise e






