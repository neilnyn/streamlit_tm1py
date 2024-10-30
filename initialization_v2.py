import streamlit as st
import psycopg2
from TM1py import TM1Service
from TM1py.Objects import Dimension,Hierarchy,Element,Cube
from collections import defaultdict




# 获取表的列名与数据
def preview_data(conn:'Psql Connection',table_name, limit=10):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return columns, data

# 显示数据
def show_preview(conn:'Psql Connection'):
    tables = ["product_zl","product_dimension", "store_dimension", "time_dimension", "orders"]
    selected_table = st.selectbox("选择要预览的表", tables)
    columns, data = preview_data(conn,selected_table)
    st.write(f"{selected_table} 预览:")
    st.text(columns)
    st.table(data)

# 通过页面选择创建维度和cube,返回维度名称，cube名称，度量值名称
def create_drag_interface(conn:'Psql Connection'):
    st.header("Define Dimensions and Measures")

    # 建立与数据库链接
    cursor = conn.cursor()


    # 创建两列布局
    col1, col2 = st.columns(2)
    cursor = conn.cursor()
    with col1:
        st.subheader("Rows (Dimensions)")
        table_name = st.selectbox("选择维表",["product_zl","product_dimension", "store_dimension", "time_dimension"])
        dimension_name = st.text_input(label='请输入新维度名称')
        dimension_table_columns_mapping=defaultdict(list)

        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        all_columns = [row[0] for row in cursor.fetchall()]
        measures=st.multiselect("选择字段", [col for col in all_columns])
        dimension_table_columns_mapping[table_name].append(measures)
    with col2:
        st.subheader("Columns (Measures)")
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders'")
        all_columns = [row[0] for row in cursor.fetchall()]
        measure_dimension_name = st.text_input(label='请输入Measure维度名称')
        measures = st.multiselect("选择度量",[col for col in all_columns])

    return dimension_name,dimension_table_columns_mapping, measure_dimension_name,measures

# 创建维度 - 数据库适配
def create_tm1_dimension_from_database(conn:'Qsql Connection',tm1:TM1Service,dim_name, dimension_mapping:dict):
    #获取数据
    cursor = conn.cursor()
    table_name = dimension_mapping.keys()
    table_name = list(table_name)[0]
    columns = dimension_mapping.get(table_name)[0]
    str_columns = ''
    for col in columns:
        str_columns = str_columns + ',' + col
    str_columns = str_columns[1:]
    cursor.execute(f"SELECT {str_columns} FROM {table_name}")
    data = cursor.fetchall()

    # 判断维度是否存在
    if not tm1.dimensions.exists(dim_name):
        new_dimension = Dimension(dim_name)
        new_hierarchy = Hierarchy(dim_name, dim_name)
        new_dimension.add_hierarchy(new_hierarchy)
        tm1.dimensions.create(new_dimension)

    # Get the hierarchy
    hierarchy = tm1.dimensions.hierarchies.get(dim_name, dim_name)
    # Get Existing Edges & # Get Existing Element
    existing_edges_set=set()
    existing_elements_set = set()
    hierarchy_data_edges_set=set()
    hierarchy_data_elements_set = set()
    if len(hierarchy.edges)>0:
        existing_edges = [key for key in hierarchy.edges]
        existing_edges_set = set(existing_edges)
        for ele in hierarchy.elements:
            existing_elements_set.add(ele)
    # hierarchy_data from database
    if len(data)>0:
        hierarchy_data = []
        for tup in data:
            for i in range(len(tup)-1):
                if tup[i] !='' and tup[i+1]!='':
                    hierarchy_data.append((tup[i],tup[i+1]))
            for ele in tup:
                if ele != '':
                    hierarchy_data_elements_set.add(ele)
        hierarchy_data_edges_set = set(hierarchy_data)

    # edge need to unwind
    edges_need_to_unwind = existing_edges_set - hierarchy_data_edges_set

    # edge need to update
    edges_need_to_update = hierarchy_data_edges_set - existing_edges_set

    # elements need to add
    elements_need_to_update = hierarchy_data_elements_set - existing_elements_set

    # unwind elements which need to
    for edge in edges_need_to_unwind:
        hierarchy.remove_edge(parent=edge[1],component=edge[0])
    # add elements which need to
    for ele in elements_need_to_update:
        try:
            hierarchy.add_element(ele, 'Numeric')
        except ValueError:
            print(f"Warning: Could not add element {ele}")
            continue

    # Update edges which need to
    for edge in edges_need_to_update:
        hierarchy.add_edge(parent=edge[1],component=edge[0],weight=1)

    # Update the hierarchy
    tm1.dimensions.hierarchies.update(hierarchy)
    # return success flag
    st.success('创建维度成功', icon="✅")

# 创建维度 from list
def create_tm1_dimension_from_list(tm1:TM1Service,dim_name,data:list):
    # 判断维度是否存在
    if not tm1.dimensions.exists(dim_name):
        new_dimension = Dimension(dim_name)
        new_hierarchy = Hierarchy(dim_name, dim_name)
        new_dimension.add_hierarchy(new_hierarchy)
        tm1.dimensions.create(new_dimension)

        # Get the hierarchy
        hierarchy = tm1.dimensions.hierarchies.get(dim_name, dim_name)
        # Get Existing Edges & # Get Existing Element
        existing_elements_set = set()
        hierarchy_data_elements_set = set()
        if len(hierarchy.elements) > 0:
            for ele in hierarchy.elements:
                existing_elements_set.add(ele)
        # hierarchy_data from database
        if len(data) > 0:
            for ele in data:
                hierarchy_data_elements_set.add(ele)
        # elements need to add
        elements_need_to_update = hierarchy_data_elements_set - existing_elements_set

        # add elements which need to
        for ele in elements_need_to_update:
            try:
                hierarchy.add_element(ele, 'Numeric')
            except ValueError:
                print(f"Warning: Could not add element {ele}")
                continue

        # Update the hierarchy
        tm1.dimensions.hierarchies.update(hierarchy)


# 创建cube
def create_tm1_cube(tm1:TM1Service,cube_name, dimensions):
    if not tm1.cubes.exists(cube_name):
        new_cube = Cube(name=cube_name, dimensions=dimensions)
        tm1.cubes.create(new_cube)
        st.success('创建Cube成功', icon="✅")





def main():
    # PostgreSQL连接
    with psycopg2.connect(database="master", user="postgres", password="1234", host="localhost", port="5432") as conn:

        show_preview(conn)

        dimension_name,dimension_table_columns_mapping, measure_dimension_name,measures = create_drag_interface(conn)


        # TM1 Connection
        with TM1Service(address='localhost', port=30059, user='neil', password='123', ssl=False) as tm1:
            st.title("TM1 Dimension Creator")
            if st.button("从Rows创建TM1维度"):
                    create_tm1_dimension_from_database(conn, tm1, dimension_name, dimension_table_columns_mapping)
            if st.button("从Column创建TM1维度"):
                    create_tm1_dimension_from_list(tm1,measure_dimension_name,measures)
            st.title("TM1 Cube Creator")
            # 获取当前示例中的所有维度
            all_dimensions_name = tm1.dimensions.get_all_names()
            all_dimensions_name = [name for name in all_dimensions_name if "}" not in name]
            cube_name = st.text_input(label='请输入Cube名称')
            dimensions_chosen = st.multiselect(label='请选择维度', options=all_dimensions_name)
            if st.button("创建TM1 Cube"):
                create_tm1_cube(tm1,cube_name,dimensions_chosen)

if __name__ == "__main__":
    main()