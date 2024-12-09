import psycopg2
from TM1py import TM1Service
#import streamlit as st
import time
import logging
from functools import wraps
# 建立PostgreSql链接方法

#@st.cache_resource
def connect_to_postgresql(username: str, password: str):
    conn = psycopg2.connect(
        database="master",
        user=username,
        password=password,
        host="localhost",
        port="5432"
    )
    return conn

# 建立TM1链接
#@st.cache_resource
def connect_to_tm1(username: str, password: str):
    tm1 = TM1Service(address='localhost', port=30059, user=username, password=password, ssl=False)
    return tm1


# logging decorator


# 配置日志
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s -%(name)s -%(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

# 装饰器定义
def log_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug(f"Calling function '{func.__name__}' with args: {args} and kwargs: {kwargs}")
        result = func(*args, **kwargs)
        logger.debug(f"Function '{func.__name__}' returned result: {result}")
        return result
    return wrapper



