1. Establish a database connection to the mysql database
    ```python
    async def connect_to_mysql():
        # Establish a connection to the MySQL database
        connection = await aiomysql.connect(
            host="localhost",          
            port=3306,                 
            user="root",               
            password="QwE456",
            db="gp_3"
        )
    ```
2. Retrieve the database for all tables.
    ```python
    tables = await cursor.fetchall()
    table_names = [table[0] for table in tables]
    ```
    ```SQL
    SHOW TABLES
    ```
3. Check if the table survey_report exist.
    ```python
    if "survey_report" not in table_names:
        print("survey_report table not exist")
    else:
        print("survey_report table exist")
    ```
4. If the table survey_report not exist, create the table survey_report.
    ```python
        # create a dict to store the attributes
        attributes_dict = await create_attributes_list(cursor)
        # create the table survey_report
        await create_survey_report_table(cursor, attributes_dict)
        # insert the data to the survey_report table
        await insert_data_to_survey_report(cursor)
    ```
5. If the table survey_report exist, prepare the data to push into the table survey_report.

    For each employee in the result:

    a. Retrieve employee id from the survey_result table, and use the id to get the employee details from the employee_name table.

    b. Get the attribute id from the survey_result table, and use the id to retrieve every key from the survey_attribute table.

    ![alt text](./python_code/image.png)

    c. Use the attribute id to find ValueCode from the survey_result table, and use the ValueCode to get the ValueDescription from the value_set table.

    ![alt text](./python_code/image-1.png)
    ![alt text](./python_code/image-2.png)

    ```SQL
    SELECT 
        sr.EmployeeId,
        en.LastName,
        en.FirstName,
        MAX(CASE WHEN sa.AttributeName = 'AbsentEmployeeReason' THEN vs.ValueDescription END) AS AbsentEmployeeReason,
        ...
    FROM
        survey_result sr
    JOIN
        survey_attribute sa ON sr.AttributeId = sa.AttributeId
    JOIN
        value_set vs ON sr.ValueCode = vs.ValueCode
    JOIN 
        employee_name en ON sr.EmployeeId = en.Id
    GROUP BY 
        sr.EmployeeId;
    ```

    d. Prepare the data push into a list.
    ```python
    query = "SELECT AttributeName FROM survey_attribute"
    attributes_dict = {"EmployeeId": None, "FirstName": None, "LastName": None}
    for attribute in result:
        attributes_dict[attribute[0]] = None
    ```

    ![alt text](./python_code/image-3.png)

    `note: Above a, b, c steps, we need to get these datas by using async function for faster. Once we get the data, we can push it into the list.`

6. Insert data into the survey_report table.
    ```python
    survey_result_list = await query_survey_result(cursor)
    await insert_survey_result(cursor, survey_result_list)
    ```
    
7. Close the database connection.

    ```python
    cursor.close()
    conn.close()
    ```
