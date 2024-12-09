import luigi
import pandas as pd
import json
from datetime import datetime
from utils import connect_to_tm1, connect_to_postgresql
from hierarchy_method_channel import create_tm1_dimension_from_database
import mdxpy
from mdxpy import MdxBuilder,MdxHierarchySet,Member
import time
# 基础任务类
class TM1BaseTask(luigi.Task):
    # 共享参数
    tm1_username = 'neil'
    tm1_password = '123'
    pg_username = 'postgres'
    pg_password = '1234'
    log_dir = 'C:\\DBs_EDU\\Neil\\log\\pipepline_log'
    cube_name = luigi.Parameter()
    schema = luigi.Parameter(default="public")
    execution_date = luigi.DateSecondParameter(default=datetime.now())

    
    def get_tm1_connection(self):
        return connect_to_tm1(self.tm1_username, self.tm1_password)
    
    def get_pg_connection(self):
        return connect_to_postgresql(self.pg_username, self.pg_password)

# 任务1: 导出数据到PostgreSQL
class ExportToPostgres(TM1BaseTask):
    # 输出成功标记文件
    def output(self):
        formatted_time = self.execution_date.strftime('%Y%m%d_%H%M%S')
        return luigi.LocalTarget(
            f"{self.log_dir}\\export_{self.cube_name}_{formatted_time}.success"
        )
    
    def run(self):
        with self.get_tm1_connection() as tm1, self.get_pg_connection() as pg_conn:
            # 获取cube数据
            dimensions = tm1.cubes.get_dimension_names(self.cube_name)
            q = MdxBuilder.from_cube(cube=self.cube_name)
            q = q.non_empty(0)
            for dimension in dimensions:
                q = q.add_hierarchy_set_to_axis(0, MdxHierarchySet.all_leaves(dimension))
            df = tm1.cubes.cells.execute_mdx_dataframe(mdx=q)
            # df有数据，才写入PostgreSQL
            if len(df) > 0:
                # 写入PostgreSQL
                with pg_conn.cursor() as cur:
                    # 处理带有空格的cube名
                    self.cube_name = self.cube_name.replace(" ", "_")

                    # 创建表将带有空格的维度名中空格替换为下划线,df最后一列为数值型
                    #cols = ",".join([f"{col} TEXT" for col in df.columns])
                    col_list = [col for col in df.columns]
                    col_list_new = [col.replace(" ", "_") for col in col_list]
                    cols = ",".join(f"{col} TEXT" for col in col_list_new[:-1])
                    cols = cols +','+col_list_new[-1]+' NUMERIC'
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {self.schema}.{self.cube_name} (
                            {cols},
                            export_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)

                    # 写入数据
                    for _, row in df.iterrows():
                        values = ",".join([f"'{v}'" for v in row])
                        cur.execute(f"""
                            INSERT INTO {self.schema}.{self.cube_name} 
                            VALUES ({values}, '{self.execution_date}')
                        """)

                pg_conn.commit()
                time.sleep(10)
                # 创建成功标记文件
                with self.output().open('w') as f:
                    f.write('success')
                

# 任务2: 维度更新
class UpdateDimension(TM1BaseTask):
    dimension_name = 'business unit tm1py'
    table_name = 'business_unit_dimension02'
    str_columns = 'level003,level002,level001,level000'
    def requires(self):
        return ExportToPostgres(
            cube_name=self.cube_name,
            schema=self.schema,
            execution_date=self.execution_date
        )
    
    def output(self):
        formatted_time = self.execution_date.strftime('%Y%m%d_%H%M%S')
        return luigi.LocalTarget(
            f"{self.log_dir}\\dimension_update_{self.dimension_name}_{formatted_time}.success"
        )
    
    def run(self):
        with self.get_tm1_connection() as tm1, self.get_pg_connection() as pg_conn:
            create_tm1_dimension_from_database(conn=pg_conn,
                tm1=tm1,
                dim_name=self.dimension_name,
                table_name=self.table_name,
                str_columns=self.str_columns)
            time.sleep(20)
            with self.output().open('w') as f:
                f.write('success')
                


# 任务3: 数据映射导入
class ImportWithMapping(TM1BaseTask):
    mapping = luigi.DictParameter()

    def requires(self):
        return UpdateDimension(
            cube_name=self.cube_name,
            schema=self.schema,
            execution_date=self.execution_date
        )
    
    def output(self):
        formatted_time = self.execution_date.strftime('%Y%m%d_%H%M%S')
        return luigi.LocalTarget(
            f"{self.log_dir}\\import_{self.cube_name}_{formatted_time}.success"
        )
    
    def run(self):
        with self.get_tm1_connection() as tm1, self.get_pg_connection() as pg_conn:
            # 处理带有空格的cube名
            self.cube_name_no_space = self.cube_name.replace(" ", "_")
            # 从PostgreSQL读取数据
            with pg_conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.schema}.{self.cube_name_no_space}
                    WHERE export_date = '{self.execution_date}'
                """)
                columns = [desc[0] for desc in cur.description]
                # 将带有下划线的列名恢复为空格除了最后export_date列
                columns = [col.replace("_", " ") if col != 'export_date' else col for col in columns]
                data = cur.fetchall()
                df = pd.DataFrame(data, columns=columns)
                # drop export_date column
                df = df.drop(columns=['export_date'])
                # 将value列转换为数值类型
                df['value'] = pd.to_numeric(df['value'])
            
            # 应用映射关系
            for old_bu, new_bu in self.mapping.items():
                df.loc[df['business unit tm1py'] == old_bu, 'business unit tm1py'] = new_bu
            # 测试出现error
            #raise Exception("test error")
            
            # 写回TM1
            tm1.cubes.cells.write_dataframe(self.cube_name, df,dimensions=tm1.cubes.get_dimension_names(self.cube_name))
            time.sleep(10)
            with self.output().open('w') as f:
                f.write('success')


# 任务4: 数据校验
class VerifyData(TM1BaseTask):
    mapping = luigi.DictParameter()
    def requires(self):
        return ImportWithMapping(
            cube_name=self.cube_name,
            schema=self.schema,
            execution_date=self.execution_date,
            mapping=self.mapping
        )
    
    def output(self):
        formatted_time = self.execution_date.strftime('%Y%m%d_%H%M%S')
        self.cube_name_no_space = self.cube_name.replace(" ", "_")
        return luigi.LocalTarget(
            f"{self.log_dir}\\verify_{self.cube_name_no_space}_{formatted_time}.json"
        )
    
    def run(self):
        with self.get_tm1_connection() as tm1, self.get_pg_connection() as pg_conn:
            # 获取TM1当前数据
            dimensions = tm1.cubes.get_dimension_names(self.cube_name)
            q = MdxBuilder.from_cube(cube=self.cube_name)
            q = q.non_empty(0)
            for dimension in dimensions:
                q = q.add_hierarchy_set_to_axis(0, MdxHierarchySet.all_leaves(dimension))
            tm1_df = tm1.cubes.cells.execute_mdx_dataframe(mdx=q)
            
            # 获取PostgreSQL数据
            self.cube_name_no_space = self.cube_name.replace(" ", "_")
            with pg_conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM {self.schema}.{self.cube_name_no_space}
                    WHERE export_date = '{self.execution_date}'
                """)
                pg_df = pd.DataFrame(
                    cur.fetchall(), 
                    columns=[desc[0] for desc in cur.description]
                )
            pg_df['value'] = pd.to_numeric(pg_df['value'])
            # 比较数据总和
            tm1_total = tm1_df['Value'].sum()
            pg_total = pg_df['value'].sum()
            
            result = {
                "verification_date": str(self.execution_date),
                "tm1_total": float(tm1_total),
                "pg_total": float(pg_total),
                "difference": float(abs(tm1_total - pg_total)),
                "is_valid": True if abs(tm1_total - pg_total) < 0.01 else False
            }
            time.sleep(10)
        # 写入验证结果
        try:
            with self.output().open('w') as f:
                    json.dump(result, f, indent=2)
        except Exception as e:
            raise e




# 主任务: 整合所有步骤
class TM1DimensionManagementPipeline(luigi.WrapperTask):
    cube_name = luigi.Parameter()
    schema = luigi.Parameter(default="public")
    execution_date = luigi.DateSecondParameter(default=datetime.now())
    mapping = luigi.DictParameter()
    
    def requires(self):
        return VerifyData(
            cube_name=self.cube_name,
            schema=self.schema,
            execution_date=self.execution_date,
            mapping=self.mapping
        )

if __name__ == "__main__":
    luigi.build([
        TM1DimensionManagementPipeline(
            cube_name="Some Rpt Cube",
            mapping={"BU003": "SU001"}
        )
    ], local_scheduler=True)
