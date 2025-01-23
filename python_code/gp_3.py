import asyncio
import aiomysql

# Async function to connect and interact with MySQL
async def connect_to_mysql():
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

            # Print the tables
            print("Tables in the database:")
            for table in tables:
                print(table[0])
            
            cursor.execute("SELECT * FROM survey_report WHERE (SELECT survey_result.EmployeeId FROM survey_result) = survey_report.employee_id")

            if "survey_report" not in tables:
                cursor.execute("CREATE TABLE survey_report (employee_id INT, first_name VARCHAR(255), last_name VARCHAR(255), PRIMARY KEY (employee_id))")
        

        # Close the connection
        connection.close()
        
    except Exception as e:
        print(f"connect to mysql error: {str(e)}")
        print("please check:")
        print("1. MySQL server whether is running")
        print("2. username and password whether is correct")
        print("3. database gp_3 whether is exist")

# Run the asyncio loop
asyncio.run(connect_to_mysql())
