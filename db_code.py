import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')

table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

file_path = '/home/project/INSTRUCTOR.csv'
df= pd.read_csv(file_path, names=attribute_list)

df.to_sql(table_name, conn, if_exists='replace', index=False)
print('Table is ready')

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')


#In the same database STAFF, create another table called Departments.
department_table_name = 'Departments'
departments_attributes = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']
departments_file_path = '/home/project/Departments.csv'

#Create Table
department_df = pd.read_csv(departments_file_path, names=departments_attributes)
print('Departments Data Loaded')
print(department_df)
department_df.to_sql(department_table_name, conn, if_exists='replace', index=False)
print('Departments table created')

'''
Append the Departments table with the following information.
Attribute	Value
DEPT_ID	9
DEP_NAME	Quality Assurance
MANAGER_ID	30010
LOC_ID	L0010
'''

department_data = {
    'DEPT_ID': 9,
    'DEP_NAME': 'Quality Assurance',
    'MANAGER_ID': 30010,
    'LOC_ID': 'L0010'
}
df = pd.DataFrame([department_data])
df.to_sql(department_table_name, conn, if_exists='append', index=False)
print('Data Appended')

# View all entries
query = 'SELECT * FROM Departments'
query_output = pd.read_sql(query, conn)
print(query)
print(query_output)

# View only department names
query = 'SELECT DEP_NAME FROM Departments'
query_output = pd.read_sql(query, conn)
print(query)
print(query_output)

# Count the total entries
query = f"SELECT COUNT(*) FROM Departments"
query_output = pd.read_sql(query, conn)
print(query)
print(query_output)

conn.close()
