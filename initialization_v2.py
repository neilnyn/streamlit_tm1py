import pandas as pd
import streamlit as st
from TM1py.Objects import Dimension,Hierarchy,Element,Cube
from collections import defaultdict
from utils import connect_to_tm1
from utils import connect_to_postgresql


st.set_page_config(
    page_title="TM1 Initialization Tool",
    page_icon="📊",
    layout="wide"  # 使用宽屏布局
)

# 获取表的列名与数据
def preview_data(conn:'Psql Connection',table_name, limit=10):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    return columns, df

# 显示数据
def show_preview(conn:'Psql Connection'):
    with st.container():
        st.markdown("### 📋 数据预览")
        tables = {
            "channel_dimension": "📊 渠道维度",
            "product_dimension": "🛍️ 产品维度",
            "store_dimension": "🏪 店铺维度",
            "time_dimension": "⏰ 时间维度",
            "orders": "📦 订单数据",
            "business_unit_dimension01": "bu维度01",
            "business_unit_dimension02": "bu维度02"
        }
        selected_table = st.selectbox(
            "选择要预览的表",
            options=list(tables.keys()),
            format_func=lambda x: tables[x]
        )

        columns, df = preview_data(conn,selected_table)
        col1, col2 = st.columns([7, 3])

        with col1:
            st.markdown(f"##### {tables[selected_table]} 数据预览")
            st.dataframe(
                df.style.set_properties(**{
                    'background-color': 'black',
                    'color': 'white',
                    'border-color': 'black'
                }),
                height=400  # 设置固定高度
            )

        with col2:
            st.markdown("##### 📊 列信息")
            column_info = pd.DataFrame({
                "列名": columns,
                "数据类型": [str(dtype) for dtype in df.dtypes]
            })
            st.dataframe(
                column_info.style.set_properties(**{
                    'background-color': 'black',
                    'color': 'white',
                    'border-color': 'black'
                })
            )



# 创建时间维表
def create_time_dimension_table(conn):
   """创建时间维度表"""
   cursor = conn.cursor()
   sql = '''
      SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_name = 'time_dimension');
      '''
   cursor.execute(sql)
   data = cursor.fetchone()
   #if table not exists.
   if data[0] is False:
      # 创建时间维度表
      cursor.execute("""
          CREATE TABLE IF NOT EXISTS time_dimension (
              year_code TEXT,
              month_code TEXT,
              day_code TEXT,
              hour_code TEXT,
              original_timestamp TEXT
          )
      """)

      # 从orders表获取唯一的支付时间
      cursor.execute("SELECT DISTINCT 支付时间 FROM orders WHERE 支付时间 IS NOT NULL")
      timestamps = cursor.fetchall()

      # 插入时间维度数据
      for timestamp in timestamps:
         if timestamp[0]:
            try:
               dt = pd.to_datetime(timestamp[0])
            except Exception as e:
               print(timestamp[0]+'时间格式错误')
               continue
            cursor.execute("""
                  INSERT INTO time_dimension (year_code, month_code, day_code, hour_code, original_timestamp)
                  VALUES (%s, %s, %s, %s, %s)
              """, (
               f'Y{dt.year}',
               f'M{dt.month:02d}',
               f'D{dt.day:02d}',
               f'{dt.hour:02d}',
               timestamp[0]
            ))
   else:
      sql = '''
      select DISTINCT o.支付时间 
      from orders o
      left join time_dimension td
      on o."支付时间" = td.original_timestamp
      where td.original_timestamp is null
      '''

      # 从orders表获取唯一的支付时间
      cursor.execute(sql)
      timestamps = cursor.fetchall()

      # 插入时间维度数据
      for timestamp in timestamps:
         if timestamp[0]:
            try:
               dt = pd.to_datetime(timestamp[0])
            except Exception as e:
               print(timestamp[0] + '时间格式错误')
               continue
            cursor.execute("""
                        INSERT INTO time_dimension (year_code, month_code, day_code, hour_code, original_timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
               f'Y{dt.year}',
               f'M{dt.month:02d}',
               f'D{dt.day:02d}',
               f'{dt.hour:02d}',
               timestamp[0]
            ))
   conn.commit()



# 通过页面选择创建维度和cube,返回维度名称，cube名称，度量值名称
def create_drag_interface(conn:'Psql Connection'):
    st.markdown("## 🎯 定义维度和度量")
    # 创建数据库连接
    cursor = conn.cursor()

    # 使用两列布局，调整比例
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📊 Rows (Dimensions)")
        with st.container():
            table_name = st.selectbox(
                "选择维表",
                ["channel_dimension", "product_dimension", "store_dimension", "time_dimension", "business_unit_dimension01", "business_unit_dimension02"],
                format_func=lambda x: {
                    "channel_dimension": "📊 渠道维度",
                    "product_dimension": "🛍️ 产品维度",
                    "store_dimension": "🏪 店铺维度",
                    "time_dimension": "⏰ 时间维度",
                    "business_unit_dimension01": "bu维度01",
                    "business_unit_dimension02": "bu维度02"
                }[x]
            )
            dimension_name = st.text_input(label='请输入新维度名称')
            dimension_table_columns_mapping=defaultdict(list)

            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            all_columns = [row[0] for row in cursor.fetchall()]
            measures=st.multiselect("选择字段", [col for col in all_columns])
            dimension_table_columns_mapping[table_name].append(measures)
    with col2:
        st.markdown("### 📈 Columns (Measures)")
        with st.container():
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders'")
            all_columns = [row[0] for row in cursor.fetchall()]
            measure_dimension_name = st.text_input(label='请输入Measure维度名称')
            measures = st.multiselect("选择度量",[col for col in all_columns])

    return dimension_name,dimension_table_columns_mapping, measure_dimension_name,measures

# 创建维度 - 数据库适配
def create_tm1_dimension_from_database(conn:'Qsql Connection',tm1:'TM1 Connection',dim_name, dimension_mapping:dict):
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
        hierarchy.add_edge(parent=edge[0],component=edge[1],weight=1)

    # # default add top element
    # top_element = 'All ' + dim_name + ' List'
    # if top_element not in hierarchy.elements:
    #     hierarchy.add_element(top_element,'Consolidated')
    # # all leaves add to top element
    # # 初始化集合
    # left_elements = set()
    # right_elements = set()
    # edges = hierarchy.edges
    # for key in edges.keys():
    #     left, right = key
    #     left_elements.add(left)
    #     right_elements.add(right)
    #
    # # 找出只在右侧出现的元素,也是所有叶子节点
    # only_right_elements = right_elements - left_elements
    # for element in only_right_elements:
    #     hierarchy.add_edge(parent=top_element, component=element, weight=1)

    # some info
    st.text({'edges_need_to_unwind':edges_need_to_unwind,'edges_need_to_update':edges_need_to_update,'elements_need_to_update':elements_need_to_update})
    #st.text({'existing_edges_set':existing_edges_set,'hierarchy_data_edges_set':hierarchy_data_edges_set})
    # Update the hierarchy
    tm1.dimensions.hierarchies.update(hierarchy)
    # return success flag
    st.success('创建维度成功', icon="✅")

# 创建维度 from list
def create_tm1_dimension_from_list(tm1:'TM1 Connection',dim_name,data:list):
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
        # default add 'count' as a measure
        hierarchy.add_element('count', 'Numeric')
        # Update the hierarchy
        tm1.dimensions.hierarchies.update(hierarchy)
        # return success flag
        st.success('创建维度成功', icon="✅")

# 创建cube
def create_tm1_cube(tm1:'TM1 Connection',cube_name, dimensions):
    if not tm1.cubes.exists(cube_name):
        new_cube = Cube(name=cube_name, dimensions=dimensions)
        tm1.cubes.create(new_cube)
        st.success('创建Cube成功', icon="✅")


# 获取实时表的列名
def get_orders_columns(conn):
    """获取orders表和关联时间维表的所有列名"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'time_dimension' OR table_name = 'orders'
    """)
    columns = [col[0] for col in cursor.fetchall()]
    return columns

def get_tm1_all_dimensions(tm1:'TM1 Connection'):
    all_dimensions_name = tm1.dimensions.get_all_names()
    all_dimensions_name = [name for name in all_dimensions_name if "}" not in name]
    return all_dimensions_name
# 获取tm1中所有维度名称
def get_tm1_cube_dimensions(tm1:'TM1 Connection',cube_name):
    """获取TM1中的cube维度"""
    try:
        cube_dimensions = tm1.cubes.get_dimension_names(cube_name)
    except Exception as e:
        cube_dimensions = []
    return cube_dimensions
# 获取tm1中的所有cube名称
def get_tm1_cubes(tm1:'TM1 Connection'):
    cube_names = tm1.cubes.get_all_names()
    cube_names= [name for name in cube_names if "}" not in name]
    return cube_names
# 获取tm1 维度中的所有元素
def get_tm1_dimension_elements(tm1:'TM1 Connection',dimension_name):
    try:
        elements = tm1.elements.get_element_names(dimension_name,dimension_name)
    except Exception as e:
        elements = []
    return elements

# 对指定C节点下进行层级打散
def unwind_consolidated_element(tm1:'TM1 Connection',dimension_name,element:str):
    hierarchy_target = tm1.dimensions.hierarchies.get(dimension_name,dimension_name)
    element_obj = tm1.elements.get(dimension_name=dimension_name,hierarchy_name=dimension_name,element_name=element)

    # 是C节点才进行unwind操作
    if element_obj.element_type.name == 'CONSOLIDATED':
        # 获取某C节点下的edges信息
        descendant_edges = hierarchy_target.get_descendant_edges(element_name=element,recursive=True)
        for key in descendant_edges:
            # 执行unwind操作
            hierarchy_target.remove_edge(key[0],key[1])
        tm1.dimensions.hierarchies.update(hierarchy_target)
        st.success('unwind维度成功', icon="✅")
# 导入tm1数据
def import_tm1_data(conn:'Psql Connection',tm1:'TM1 Connection',cube_name,cube_dimensions:list,dimension_mapping:dict,measure_elements,measure_mapping:dict):
    cursor = conn.cursor()
    all_data = dict()
    # 拼出需要查询的sql字符串
    sql_columns = ''
    sql_columns_and_measure = ''
    for dimension in cube_dimensions[:-1]:
        sql_columns = sql_columns + ',' + dimension_mapping.get(dimension)     # 拼接列名
    sql_columns = sql_columns[1:]
    for measure in measure_elements[:-1]:
        sql_columns_and_measure = sql_columns + ',' + 'sum('+measure_mapping.get(measure)+') as '+measure_mapping.get(measure)
        cursor.execute(f'''SELECT {sql_columns_and_measure} FROM time_dimension LEFT JOIN orders ON time_dimension.original_timestamp = orders.支付时间
        GROUP BY {sql_columns} ''')
        #st.write(f"SELECT {sql_columns_and_measure} FROM time_dimension LEFT JOIN orders ON time_dimension.original_timestamp = orders.支付时间")
        data = cursor.fetchall()
        # 将每一行转换为list可变对象后，追加measure字段
        data_new = dict()
        for row in data:
            row_list = list(row)
            row_list.insert(-1, measure)
            row_list_without_measure = row_list[:-1]            # 去除measure字段
            new_tuple_without_measure = tuple(row_list_without_measure)
            measure_value = row_list[-1]
            data_new[new_tuple_without_measure] = measure_value
        all_data.update(data_new)
    # 只记录数据行组合出现的次数
    cursor.execute(f'''SELECT {sql_columns},count(*) as count FROM time_dimension LEFT JOIN orders ON time_dimension.original_timestamp = orders.支付时间
    GROUP BY {sql_columns} ''')
    data_count = cursor.fetchall()
    data_count_new = dict()
    for row in data_count:
        row_list = list(row)
        row_list.insert(-1, 'count')
        row_list_without_measure = row_list[:-1]  # 去除measure字段
        new_tuple_without_measure = tuple(row_list_without_measure)
        measure_value = row_list[-1]
        data_count_new[new_tuple_without_measure] = measure_value
    # data clear
    tm1.cells.clear(cube=cube_name)
    # 导入tm1
    tm1.cells.write(cube_name=cube_name,cellset_as_dict=all_data,dimensions=cube_dimensions, use_ti=True,increment=True)
    tm1.cells.write(cube_name=cube_name, cellset_as_dict=data_count_new, dimensions=cube_dimensions, use_ti=True,increment=True)
    # success flag
    st.success('数据导入Cube成功', icon="✅")
def main():
    st.markdown("""
        # 🎯 TM1主数据管理界面
        ### PostgreSQL to TM1
    """)

    # tabs 不同的功能区域
    tab1, tab2, tab3, tab4 = st.tabs(["📊 数据预览", "🛠️ 维度管理","🛠️ Cube生成" ,"📈 数据导入"])
    # PostgreSQL连接
    #@st.cache_resource
    with connect_to_postgresql(username="postgres",password="1234") as conn:
        with connect_to_tm1(username='neil', password='123') as tm1:
            # 数据预览&创建时间维表
            with tab1:
                show_preview(conn)
                with st.expander("🕒 创建时间维度表"):
                    create_time_dim = st.checkbox("是否创建时间维度表")
                    if create_time_dim:
                        try:
                            create_time_dimension_table(conn)
                            st.success("✅ 时间维度表创建成功")
                        except Exception as e:
                            st.error(f"❌ 创建失败: {str(e)}")
            with tab2:
                # 拖拽部分
                dimension_name,dimension_table_columns_mapping, measure_dimension_name,measures = create_drag_interface(conn)

                st.markdown("### 🏗️ 创建TM1维度")
                col1, col2= st.columns(2)
                with col1:
                    if st.button("🔄 从Rows创建TM1维度"):
                            create_tm1_dimension_from_database(conn, tm1, dimension_name, dimension_table_columns_mapping)

                    with st.expander("Unwind功能"):
                            st.markdown("#### 📝 Unwind Dimension")
                            unwind_dimension_name = st.text_input(label='请输入unwind目标维度名称')
                            unwind_target_element = st.text_input(label='请输入C节点元素名称')
                            if st.button("🔄 Unwind TM1维度"):
                                unwind_consolidated_element(tm1,unwind_dimension_name,unwind_target_element)
                with col2:
                    if st.button("🔄 从Column创建TM1维度"):
                            create_tm1_dimension_from_list(tm1,measure_dimension_name,measures)




            with tab3:
                st.markdown("### 📥 Cube创建")
                # 获取当前示例中的所有维度
                all_dimensions_name = get_tm1_all_dimensions(tm1)
                cube_name = st.text_input(label='请输入Cube名称')
                dimensions_chosen = st.multiselect(label='请选择维度', options=all_dimensions_name)
                if st.button("创建TM1 Cube"):
                    create_tm1_cube(tm1,cube_name,dimensions_chosen)

            with tab4:
                st.subheader("Data Import")
                st.markdown("#### 维度映射")
                target_cube_name = st.text_input(label='请输入目标Cube名称')
                target_cube_dimensions_name = get_tm1_cube_dimensions(tm1,target_cube_name)
                all_columns = get_orders_columns(conn)
                st.write("请对事实表中的字段选择对应的维度")
                # 存储tm1维度和事实表字段映射关系
                dimension_mapping = {}
                # 为目标cube维度创建一个下拉选择框
                for dim in target_cube_dimensions_name[:-1]:
                    selected_column = st.selectbox(
                        f"为维度 '{dim}' 选择对应的列",
                        [""] + all_columns
                    )
                    if selected_column:
                        dimension_mapping[dim] = selected_column
                st.write("维度映射关系：", dimension_mapping)

                # 存储tm1 measures和事实表的映射关系
                measure_mapping = {}
                # 选择measure 维度并返回维度元素
                measure_dimension_name = st.selectbox('请挑选measure维度',target_cube_dimensions_name)
                measure_elements = get_tm1_dimension_elements(tm1,measure_dimension_name)
                for measure_element in measure_elements[:-1]:
                    selected_column = st.selectbox(
                        f"为measure '{measure_element}' 选择对应的列",
                        [""] + all_columns
                    )
                    if selected_column:
                        measure_mapping[measure_element] = selected_column
                st.write("Measure映射关系：", measure_mapping)

                # 导入数据
                if st.button("导入数据"):
                    target_cube_dimensions = get_tm1_cube_dimensions(tm1,target_cube_name)
                    import_tm1_data(conn,tm1,target_cube_name,target_cube_dimensions,dimension_mapping,measure_elements,measure_mapping)


if __name__ == "__main__":
    main()