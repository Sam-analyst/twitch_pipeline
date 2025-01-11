import os

def read_sql(sql_file_path) -> str:

    current_dir = os.path.dirname(os.path.realpath(__file__))

    project_base_dir = os.path.abspath(os.path.join(current_dir, '..'))

    full_path = os.path.join(project_base_dir, 'include', sql_file_path)
    
    with open(full_path, "r") as file:
        sql = file.read()
    
    return sql