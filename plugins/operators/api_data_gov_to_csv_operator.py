from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 

class DatagovApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path','file_name','base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'data.gov'
        self.path = path
        self.file_name = file_name
        self.endpoint = dataset_nm + '{{var.value.apikey_data_gov}}' 
        self.base_dt = base_dt

    def execute(self, context):
        import os
        
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}/{self.endpoint}'
        school_df = self._call_api(self.base_url)
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        school_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url):
        import requests
        import json 

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        request_url = f'{base_url}'
        if self.base_dt is not None:
            request_url = f'{base_url}/{self.base_dt}'
        response = requests.get(request_url, headers)
        contents = json.loads(response.text)

        emp_dict={}
        for result in range(0,len(contents["results"])):
            school_name = contents["results"][result]["school"]["name"]
            emp_dict[school_name] = contents["results"][result]["school"]
        school_df = pd.DataFrame.from_dict(emp_dict, orient='index')
    

        return school_df