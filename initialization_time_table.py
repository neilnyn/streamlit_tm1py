import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime
from TM1py import TM1Service


def create_db_connection():
    """创建PostgreSQL数据库连接"""
    return psycopg2.connect(
        dbname="your_database",
        user="your_user",
        password="your_password",
        host="your_host",
        port="5432"
    )


def create_time_dimension_table(conn):
    """创建时间维度表"""
    cursor = conn.cursor()

    # 创建时间维度表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS time_dimension (
            time_id SERIAL PRIMARY KEY,
            year_code TEXT,
            month_code TEXT,
            day_code TEXT,
            hour_code TEXT,
            original_timestamp TIMESTAMP
        )
    """)

    # 从orders表获取唯一的支付时间
    cursor.execute("SELECT DISTINCT 支付时间 FROM orders WHERE 支付时间 IS NOT NULL")
    timestamps = cursor.fetchall()

    # 插入时间维度数据
    for timestamp in timestamps:
        if timestamp[0]:
            dt = pd.to_datetime(timestamp[0])
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

    # 添加外键到orders表
    cursor.execute("""
        ALTER TABLE orders 
        ADD COLUMN IF NOT EXISTS time_dimension_id INTEGER,
        ADD FOREIGN KEY (time_dimension_id) 
        REFERENCES time_dimension(time_id)
    """)

    # 更新orders表的外键
    cursor.execute("""
        UPDATE orders o
        SET time_dimension_id = td.time_id
        FROM time_dimension td
        WHERE o.支付时间 = td.original_timestamp
    """)

    conn.commit()
    cursor.close()


def get_tm1_dimensions(tm1_service):
    """获取TM1中的所有维度"""
    return tm1_service.dimensions.get_all_names()


def get_orders_columns(conn):
    """获取orders表的所有列名"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'orders'
    """)
    columns = [col[0] for col in cursor.fetchall()]
    cursor.close()
    return columns


def main():
    st.title("TM1 数据集成工具")

    # 添加TM1连接配置
    with st.sidebar:
        st.subheader("TM1 连接配置")
        tm1_host = st.text_input("TM1 Host", "localhost")
        tm1_port = st.text_input("TM1 Port", "8001")
        tm1_user = st.text_input("TM1 User", "admin")
        tm1_password = st.text_input("TM1 Password", type="password")
        tm1_ssl = st.checkbox("使用SSL", True)

    # 创建TM1连接
    try:
        tm1_service = TM1Service(
            address=tm1_host,
            port=tm1_port,
            user=tm1_user,
            password=tm1_password,
            ssl=tm1_ssl
        )
        st.success("已成功连接到TM1服务器")
    except Exception as e:
        st.error(f"连接TM1失败: {str(e)}")
        return

    # 创建数据库连接
    try:
        conn = create_db_connection()
        st.success("已成功连接到PostgreSQL数据库")
    except Exception as e:
        st.error(f"连接数据库失败: {str(e)}")
        return

    # 时间维度表选项
    create_time_dim = st.checkbox("创建时间维度表")
    if create_time_dim:
        try:
            create_time_dimension_table(conn)
            st.success("时间维度表创建成功")
        except Exception as e:
            st.error(f"创建时间维度表失败: {str(e)}")

    # 获取维度映射所需的数据
    tm1_dimensions = get_tm1_dimensions(tm1_service)
    orders_columns = get_orders_columns(conn)

    # 创建维度映射界面
    st.subheader("维度映射配置")
    st.write("请为每个TM1维度选择对应的PostgreSQL列")

    # 存储映射关系
    dimension_mapping = {}

    # 为每个TM1维度创建一个下拉选择框
    for dim in tm1_dimensions:
        selected_column = st.selectbox(
            f"为维度 '{dim}' 选择对应的列",
            [""] + orders_columns,
            key=f"dim_{dim}"
        )
        if selected_column:
            dimension_mapping[dim] = selected_column

    # 保存映射按钮
    if st.button("保存映射"):
        if dimension_mapping:
            st.json(dimension_mapping)
            st.success("维度映射已保存")
        else:
            st.warning("请至少创建一个维度映射")

    # 关闭连接
    conn.close()
    tm1_service.logout()


if __name__ == "__main__":
    main()