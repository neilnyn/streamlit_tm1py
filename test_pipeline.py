from tm1_luigi_pipeline import ExportToPostgres,UpdateDimension,ImportWithMapping,VerifyData
import luigi
import logging
import requests
import datetime
# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# if __name__ == "__main__":
#     # 执行导出任务
#     luigi.build([
#         ExportToPostgres(
#             cube_name="Some Rpt Cube",  # 确保这是TM1中实际存在的cube名称
#             schema="public"  # PostgreSQL的schema名称
#         )
#     ], local_scheduler=True)
# if __name__ == "__main__":
#     luigi.build([
#         UpdateDimension(
#             cube_name="Some Rpt Cube"
#         )
#     ], local_scheduler=True)
# if __name__ == "__main__":
#     luigi.build([
#         ImportWithMapping(
#             cube_name="Some Rpt Cube",
#             mapping={"BU003": "SU001"}  # 使用测试mapping
#         )
#     ], local_scheduler=True)


# if __name__ == "__main__":
#     luigi.build([
#         VerifyData(
#             cube_name="Some Rpt Cube",
#             mapping={"BU003": "SU001"}  # 使用测试mapping
#         )
#     ], local_scheduler=True)


    # class VerifyData(TM1BaseTask):
    #     mapping = luigi.DictParameter()
    #
    #     def requires(self):
    #         return ImportWithMapping(
    #             cube_name=self.cube_name,
    #             schema=self.schema,
    #             execution_date=self.execution_date,
    #             mapping=self.mapping
    #         )
    #
    #     def output(self):
    #         formatted_time = self.execution_date.strftime('%Y%m%d_%H%M%S')
    #         self.cube_name_no_space = self.cube_name.replace(" ", "_")
    #         return luigi.LocalTarget(
    #             f"{self.log_dir}\\verify_{self.cube_name_no_space}_{formatted_time}.json"
    #         )
    #
    #     def run(self):
    #         try:
    #             with self.get_tm1_connection() as tm1, self.get_pg_connection() as pg_conn:
    #                 # 获取TM1当前数据
    #                 dimensions = tm1.cubes.get_dimension_names(self.cube_name)
    #                 q = MdxBuilder.from_cube(cube=self.cube_name)
    #                 q = q.non_empty(0)
    #                 for dimension in dimensions:
    #                     q = q.add_hierarchy_set_to_axis(0, MdxHierarchySet.all_leaves(dimension))
    #                 tm1_df = tm1.cubes.cells.execute_mdx_dataframe(mdx=q)
    #
    #                 # 获取PostgreSQL数据
    #                 self.cube_name_no_space = self.cube_name.replace(" ", "_")
    #                 with pg_conn.cursor() as cur:
    #                     cur.execute(f"""
    #                         SELECT * FROM {self.schema}.{self.cube_name_no_space}
    #                         WHERE export_date = '{self.execution_date}'
    #                     """)
    #                     pg_df = pd.DataFrame(
    #                         cur.fetchall(),
    #                         columns=[desc[0] for desc in cur.description]
    #                     )
    #                 pg_df['value'] = pd.to_numeric(pg_df['value'])
    #
    #                 # 比较数据总和
    #                 tm1_total = tm1_df['Value'].sum()
    #                 pg_total = pg_df['value'].sum()
    #
    #                 result = {
    #                     "verification_date": str(self.execution_date),
    #                     "tm1_total": float(tm1_total),
    #                     "pg_total": float(pg_total),
    #                     "difference": float(abs(tm1_total - pg_total)),
    #                     "is_valid": abs(tm1_total - pg_total) < 0.01
    #                 }
    #
    #             # 将文件写入操作移到数据库连接之外
    #             with self.output().open('w') as f:
    #                 json.dump(result, f, indent=2)
    #                 f.flush()  # 确保数据写入
    #
    #         except Exception as e:
    #             # 如果发生错误，确保文件句柄被关闭
    #             if 'f' in locals():
    #                 f.close()
    #             raise e


def get_task_status():
    try:
        response = requests.get('http://localhost:8082/api/task_list')
        return response.json()
    except:
        return None

re = get_task_status()
tasks = []
for task_id, task_info in re['response'].items():
    # Calculate running time
    running_time = task_info.get('last_updated', 0) - task_info.get('start_time', 0)

    tasks.append({
        'name': task_info['name'],
        'status': task_info['status'],
        'start_time': datetime.datetime.fromtimestamp(task_info['start_time']).strftime('%H:%M:%S'),
        'running_time': f"{running_time:.2f}s",
        'last_updated': datetime.datetime.fromtimestamp(task_info['last_updated']).strftime('%H:%M:%S'),
        'worker': task_info.get('worker_running', '').split('host=')[1].split(',')[0]
        if task_info.get('worker_running') else 'None'
    })