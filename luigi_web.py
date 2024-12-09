# streamlit_luigi_monitor.py
import streamlit as st
import time
import json
from datetime import datetime
import subprocess
import luigi
from tm1_luigi_pipeline import TM1DimensionManagementPipeline
import requests
import pandas as pd

class LuigiMonitor:
    def __init__(self, luigi_port=8082):
        self.luigi_port = luigi_port
        self.luigi_url = f"http://localhost:{luigi_port}/api/task_list"

    def get_task_status(self):
        try:
            response = requests.get(self.luigi_url)
            data = response.json()

            # Extract task information from response
            tasks = []
            for task_id, task_info in data['response'].items():
                # Calculate running time
                running_time = task_info.get('last_updated', 0) - task_info.get('start_time', 0)

                tasks.append({
                    'name': task_info['name'],
                    'status': task_info['status'],
                    'start_time': datetime.fromtimestamp(task_info['start_time']).strftime('%H:%M:%S'),
                    'running_time': f"{running_time:.2f}s",
                    'last_updated': datetime.fromtimestamp(task_info['last_updated']).strftime('%H:%M:%S'),
                    'worker': task_info.get('worker_running', '').split('host=')[1].split(',')[0]
                    if task_info.get('worker_running') else 'None'
                })

            return tasks
        except Exception as e:
            st.error(f"Error getting task status: {str(e)}")
            return None
def run_pipeline(cube_name, mapping):
    try:
        subprocess.Popen([
            "luigid"
        ])
        time.sleep(2)  # Wait for Luigi daemon to start

        task = TM1DimensionManagementPipeline(
            cube_name=cube_name,
            mapping=mapping
        )
        luigi.build([task], local_scheduler=False)
        return True
    except Exception as e:
        st.error(f"Error starting pipeline: {str(e)}")
        return False


def main():
    # Check if pipeline is already running
    if 'pipeline_running' not in st.session_state:
        st.session_state.pipeline_running = False

    st.set_page_config(
        page_title="Luigi Pipeline Monitor",
        page_icon="🔄",
        layout="wide"
    )

    st.title("Luigi Pipeline Monitor")

    # 侧边栏控制区
    with st.sidebar:
        st.header("Control Panel")
        cube_name = st.text_input("Cube Name", value="Some Rpt Cube")
        mapping_input = st.text_area(
            "Mapping (JSON format)",
            value='{"BU003": "SU001"}',
            help="e.g., {\"old_value\": \"new_value\"}"
        )
        if st.button("Start Pipeline"):
            mapping = json.loads(mapping_input)
            if run_pipeline(cube_name, mapping):
                st.session_state.pipeline_running = True
                st.success("Pipeline started!")

    # 主体区域
    st.header("Pipeline Status")
    if st.session_state.pipeline_running:
        status_placeholder = st.empty()
        monitor = LuigiMonitor()
        while True:
            task_status = monitor.get_task_status()

            if task_status:
                status_df = pd.DataFrame(task_status)
                with status_placeholder.container():
                    st.dataframe(status_df)
            # 每5秒刷新一次
            progress_text = "each 5 seconds to refresh the status"
            my_bar = st.progress(0, text=progress_text)

            for percent_complete in range(5):
                time.sleep(1)
                my_bar.progress(percent_complete + 1, text=progress_text)
            my_bar.empty()






if __name__ == "__main__":
    main()