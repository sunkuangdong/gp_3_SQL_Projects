import asyncio
import aiomysql

# create a dict to store the attributes
async def create_attributes_list(cursor):
    query = "SELECT AttributeName FROM survey_attribute"
            
    await cursor.execute(query)
    result = await cursor.fetchall()
    attributes_dict = {}
    attributes_dict.update({"EmployeeId": None, "FirstName": None, "LastName": None})
    for attribute in result:
        attributes_dict[attribute[0]] = None
    return attributes_dict
# create the table survey_report
async def create_survey_report_table(cursor, attributes_dict):
    query = """
            CREATE TABLE survey_report (
            """
    columns = []
    for attribute in attributes_dict:
        if attribute == "EmployeeId":
            columns.append(f"{attribute} INT PRIMARY KEY")
            columns.append("FOREIGN KEY (EmployeeId) REFERENCES employee_name(Id)")
        else:
            columns.append(f"{attribute} VARCHAR(255)")

    query += ", ".join(columns)
    query += ")"
    await cursor.execute(query)
# query the survey result
async def query_survey_result(cursor):
    query = """
            SELECT 
                sr.EmployeeId,
                en.LastName,
                en.FirstName,
                MAX(CASE WHEN sa.AttributeName = 'AbsentEmployeeReason' THEN vs.ValueDescription END) AS AbsentEmployeeReason,
                MAX(CASE WHEN sa.AttributeName = 'Certifications' THEN vs.ValueDescription END) AS Certifications,
                MAX(CASE WHEN sa.AttributeName = 'Cohort' THEN vs.ValueDescription END) AS Cohort,
                MAX(CASE WHEN sa.AttributeName = 'Department' THEN vs.ValueDescription END) AS Department,
                MAX(CASE WHEN sa.AttributeName = 'EmployeeAdmins' THEN vs.ValueDescription END) AS EmployeeAdmins,
                MAX(CASE WHEN sa.AttributeName = 'Gender' THEN vs.ValueDescription END) AS Gender,
                MAX(CASE WHEN sa.AttributeName = 'HireGroups' THEN vs.ValueDescription END) AS HireGroups,
                MAX(CASE WHEN sa.AttributeName = 'ItemizedFunction' THEN vs.ValueDescription END) AS ItemizedFunction,
                MAX(CASE WHEN sa.AttributeName = 'ItemizedIndustry' THEN vs.ValueDescription END) AS ItemizedIndustry,
                MAX(CASE WHEN sa.AttributeName = 'JobAcceptanceDetails' THEN vs.ValueDescription END) AS JobAcceptanceDetails,
                MAX(CASE WHEN sa.AttributeName = 'OfficeLocation' THEN vs.ValueDescription END) AS OfficeLocation,
                MAX(CASE WHEN sa.AttributeName = 'OutreachCoach' THEN vs.ValueDescription END) AS OutreachCoach,
                MAX(CASE WHEN sa.AttributeName = 'OutreachStatus' THEN vs.ValueDescription END) AS OutreachStatus,
                MAX(CASE WHEN sa.AttributeName = 'RetireDate' THEN vs.ValueDescription END) AS RetireDate,
                MAX(CASE WHEN sa.AttributeName = 'StopFeed' THEN vs.ValueDescription END) AS StopFeed,
                MAX(CASE WHEN sa.AttributeName = 'TrackingCompleted' THEN vs.ValueDescription END) AS TrackingCompleted,
                MAX(CASE WHEN sa.AttributeName = 'WorkExperienceFunction' THEN vs.ValueDescription END) AS WorkExperienceFunction,
                MAX(CASE WHEN sa.AttributeName = 'WorkExperienceIndustry' THEN vs.ValueDescription END) AS WorkExperienceIndustry,
                MAX(CASE WHEN sa.AttributeName = 'YearsofExperience' THEN vs.ValueDescription END) AS YearsofExperience
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
            """
    await cursor.execute(query)
    result = await cursor.fetchall()
    return result
# insert the survey result
async def insert_survey_result(cursor, survey_result_list): 
    query = """
            INSERT INTO survey_report (EmployeeId, LastName, FirstName, AbsentEmployeeReason, Certifications, Cohort, Department, EmployeeAdmins, Gender, HireGroups, ItemizedFunction, ItemizedIndustry, JobAcceptanceDetails, OfficeLocation, OutreachCoach, OutreachStatus, RetireDate, StopFeed, TrackingCompleted, WorkExperienceFunction, WorkExperienceIndustry, YearsofExperience)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
    await cursor.executemany(query, survey_result_list)
    await cursor.execute("COMMIT")
# query the survey report
async def query_survey_report(cursor):
    query = """
            SELECT * FROM survey_report
            """
    await cursor.execute(query)
    result = await cursor.fetchall()
    return result
# insert the data to the survey_report table
async def insert_data_to_survey_report(cursor):
    survey_result_list = await query_survey_result(cursor)
    # create the table survey_report
    await create_attributes_list(cursor)
    # insert the survey result
    await insert_survey_result(cursor, survey_result_list)
    # query the survey result
    # await query_survey_report(cursor)


# Async function to connect and interact with MySQL
async def connect_to_mysql():
    connection = None
    try:
        # Establish a connection to the MySQL database
        connection = await aiomysql.connect(
            host="localhost",          
            port=3306,                 
            user="root",               
            password="QwE456",
            db="gp_3"
        )

        # Create a cursor for SQL operations
        async with connection.cursor() as cursor:
            # Example: Execute a query to fetch all tables in the database
            await cursor.execute("SHOW TABLES")
            tables = await cursor.fetchall()
            table_names = [table[0] for table in tables]
            # check if the table survey_report is exist
            if "survey_report" not in table_names:
                # create a dict to store the attributes
                attributes_dict = await create_attributes_list(cursor)
                # create the table survey_report
                await create_survey_report_table(cursor, attributes_dict)
                # insert the data to the survey_report table
                await insert_data_to_survey_report(cursor)
            else:
                # insert the data to the survey_report table
                await insert_data_to_survey_report(cursor)




    except Exception as e:
        print(f"connect to mysql error: {str(e)}")
        print("please check:")
        print("1. MySQL server whether is running")
        print("2. username and password whether is correct")
        print("3. database gp_3 whether is exist")
    
    finally:
        if connection:
            connection.close()

# Run the asyncio loop
asyncio.run(connect_to_mysql())
