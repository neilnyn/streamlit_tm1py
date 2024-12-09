
from TM1py import TM1Service, Rules
import logging
import argparse
import configparser
import re
import streamlit as st
from utils import connect_to_tm1

st.set_page_config(
    page_title="TM1 Security Manager",
    page_icon="🔐",
    layout="wide"
)


logging.basicConfig(level=logging.INFO,format='%(asctime)s -%(name)s -%(levelname)s - %(message)s')


# 获取列表，并将列表的文本组合成长字符串用换行符链接
def handle_rule_list(li: list):
    s = ''
    cnt = 1
    n = len(li)
    for l in li:
        if cnt == 1:
            s = l
        elif cnt == n:
            s = s + ';'+'\n' + l + ';'
        else:
            s = s + ';'+'\n' + l
        cnt = cnt + 1
    return s


# 获取某维度中某个节点下全部的项目
def descendants(tm1: TM1Service, dim: str, element: str):
    hier = tm1.hierarchies.get(dimension_name=dim, hierarchy_name=dim)
    desc = hier.get_descendants(element_name=element, recursive=True)
    ele = tm1.elements.get(dim, dim, element)
    desc = list(desc)
    desc.append(ele)
    return desc


# 获取某cube中的rules,忽略skipcheck&feeders特定开头结尾，并处理成列表
def rules(tm1: TM1Service, cube: str):
    cube = tm1.cubes.get(cube)
    rules = cube.rules
    if rules is None:
        rule_list = []
    else:
        rule_list = rules.rule_statements
        # 忽略skipcheck
        if str.lower(rule_list[0]) == 'skipcheck':
            rule_list.pop(0)
        # 忽略feeders
        if str.lower(rule_list[-1]) == 'feeders':
            rule_list.pop(-1)
    return rule_list


# 更新某cube的rule
def update_rules(tm1: TM1Service, rules_str: str, cube: str):
    try:
        new_rules = Rules(rules_str)
        cube = tm1.cubes.get(cube)
        cube.rules = new_rules
        tm1.cubes.update(cube)
    except Exception as e:
        # 写入pa日志报错信息
        tm1.server.write_to_message_log('WARN', cube + '规则更新失败')


# 新建cellsecurity控制cube（如不存在）
def create_cell_security_cube(tm1: TM1Service, cube: str):
    security_cube = '}CellSecurity_' + cube
    dim_list = tm1.cubes.get_dimension_names(cube)
    if tm1.cubes.exists(cube_name=security_cube) == False:
        p = ''
        for i in range(len(dim_list)):
            p = p + '1:'
        p = p[:-1]
        tm1.processes.execute_ti_code(["CellSecurityCubeCreate('" + cube + "','" + p + "');"])


# 根据某维度，某个汇总节点创建下属所有的数据权限组,并修改相关cube的cellsecurity
def create_groups_and_update_securityrules(tm1: TM1Service, dim: str, element: str, group_prefix: str, cube: str):
    # 创建cellsecurity_cube,如果不存在
    create_cell_security_cube(tm1, cube)
    security_cube = '}CellSecurity_' + cube
    desc = descendants(tm1, dim, element)
    # 同时生成rule的列表
    new_rule_list = []
    for ele in desc:
        element_exist_judge = tm1.elements.exists(dimension_name='}Groups', hierarchy_name='}Groups',
                                                  element_name=group_prefix + ':' + ele.name)
        if element_exist_judge == False:
            # 更新}group权限组
            try:
                tm1.security.create_group(group_prefix + ':' + ele.name)
            except Exception as e:
                logging.error('报错如下：' + str(e) + '  结束')
                # 写入pa日志报错信息
                tm1.server.write_to_message_log('WARN',
                                                '更新}Group的' + dim + '权限的组失败:' + group_prefix + ':' + ele.name)
        new_rule_list.append(
            "['" + group_prefix + ":" + ele.name + "']" + "=S:" + "IF((ELISANC(" + "'" + dim + "'" + "," + "'" + ele.name + "'" + "," + "!" + dim + ")" + "<>0 % " + "!" + dim + "@=" + "'" + ele.name + "'" + ")" + "," + \
            "'READ',CONTINUE)")
    # 现存rule的获取
    existing_rule_list = rules(tm1, security_cube)
    # 将现存rule中的"[]=S:'NONE'"移除
    for r in existing_rule_list:
        # 先除去空格
        if re.sub(' ', '', r) == "[]=S:'NONE'":
            existing_rule_list.remove(r)

    # 整合新老rules
    pre_updating_rule_list = existing_rule_list + ['#以下规则为系统自动写入'] + new_rule_list
    pre_updating_rule_list = (map(str.lower, pre_updating_rule_list))
    # 整合后去重
    seen = set()
    pre_updating_rule_unique_list = [x for x in pre_updating_rule_list if not (x in seen or seen.add(x))]
    pre_updating_rule_unique_list.insert(0, 'SKIPCHECK')
    # 最后填加cellsecurity rules”笔封“
    pre_updating_rule_unique_list.append("[]=S:'NONE'")
    pre_updating_rule_unique_list.append('FEEDERS')
    pre_updating_rule_str = handle_rule_list(pre_updating_rule_unique_list)
    # 更新rules
    update_rules(tm1, pre_updating_rule_str, security_cube)

def get_tm1_all_dimensions(tm1:'TM1 Connection'):
    all_dimensions_name = tm1.dimensions.get_all_names()
    all_dimensions_name = [name for name in all_dimensions_name if "}" not in name]
    return all_dimensions_name

def get_tm1_dimension_elements(tm1:'TM1 Connection',dimension_name):
    try:
        elements = tm1.elements.get_element_names(dimension_name,dimension_name)
    except Exception as e:
        elements = []
    return elements

def get_tm1_cubes(tm1:'TM1 Connection'):
    cube_names = tm1.cubes.get_all_names()
    cube_names= [name for name in cube_names if "}" not in name]
    return cube_names

def main():
    st.markdown("""
        # 🎯 自定义数据权限组
        ### 
    """)
    with st.expander("ℹ️ 使用说明", expanded=True):
        st.markdown("""
            1. 选择需要管控的维度（Security Dimension）
            2. 选择维度汇总元素（Elements）
            3. 输入权限组前缀（Group Prefix）
            4. 选择需要应用安全规则的Cube（Cubes）
            5. 点击创建按钮完成配置
        """)
    with connect_to_tm1(username='neil', password='123') as tm1:
        st.markdown("### 📊 基本配置")
        all_dimensions_name = get_tm1_all_dimensions(tm1)
        security_dimension = st.selectbox(label='请选择security_dimension', options=all_dimensions_name)
        if security_dimension:
            elements = get_tm1_dimension_elements(tm1, security_dimension)
            element_code = st.selectbox(label='请选择element_code', options=elements )
        data_security_prefix = st.text_input(label='请输入data_security_prefix')
        all_cubes = get_tm1_cubes(tm1)
        security_cube = st.selectbox(label='请选择security_cube', options=all_cubes)


        if st.button("🔒 创建安全配置", help="点击创建权限组和更新安全规则"):
            # 验证输入
            if not all([security_dimension, element_code, data_security_prefix, security_cube]):
                st.warning("⚠️ 请填写所有必要的配置项")
            else:
                try:
                    with st.spinner("⏳ 创建中..."):
                        create_groups_and_update_securityrules(tm1, security_dimension, element_code,
                                                   data_security_prefix,security_cube)
                        st.success("✅ 创建成功")
                except Exception as e:
                    st.error(f"❌ 创建失败: {str(e)}")

if __name__ == '__main__':
    main()






