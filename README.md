1. Establish a database connection to the mysql database

2. Create a survey_report table if it doesn't exist. If it does, we need to insert the data into the table, we don't need to create it again.
    a. we need to async create this table for faster.

    ```sql
    CREATE TABLE IF NOT EXISTS survey_report (
        primary key employee_id INT,          // employee_name table
        first_name VARCHAR(255),              // employee_name table
        last_name VARCHAR(255),               // employee_name table
    );
    ```

3. Initialize an empty list to store the data

    ```python
    survey_report_data = [
        {
            employee_id: 1,
            first_name: "John",
            last_name: "Doe",
            ...
        },
        ...
    ]
    ```

4. For each employee in the result:

    a. Retrieve employee id from the survey_result table, and use the id to get the employee details from the employee_name table.

        ```python
        employee_id = survey_result.employee_id
        employee_details = employee_name.get(employee_id)
        ```
    b. Get the attribute id from the survey_result table, and use the id to retrieve every key from the survey_attribute table.

        ```python
        attribute_id = survey_result.attribute_id
        attribute_keys = survey_attribute.get(attribute_id)
        ```

    ![alt text](image.png)

    c. Use the attribute id to find ValueCode from the survey_result table, and use the ValueCode to get the ValueDescription from the value_set table.

        ```python
        value_code_id = survey_result.value_code_id
        value_description = value_set.get(value_code_id)
        ```
    ![alt text](image-1.png)
    ![alt text](image-2.png)

    d. Prepare the data push into a list.

        ```python
        list_data = [{
                employee_id: employee_details.employee_id,
                first_name: employee_details.first_name,
                last_name: employee_details.last_name,
                AbsentEmployeeReason: value_description,
                Certifications: value_description,
                Cohort: value_description,
                Department: value_description,
                EmployeeAdmins: value_description,
                Gender: value_description,
                HireGroups: value_description,
                ItemizedFunction: value_description,
                ItemizedIndustry: value_description,
                JobAcceptanceDetails: value_description,
                OfficeLocation: value_description,
                OutreachCoach: value_description,
                OutreachStatus: value_description,
                RetireDate: value_description,
                StopFeed: value_description,
                TrackingCompleted: value_description,
                WorkExperienceFunction: value_description,
                WorkExperienceIndustry: value_description,
                YearsofExperience: value_description
            },
            ...
        ]
        ```

    ![alt text](image-3.png)

    `note: Above a, b, c steps, we need to get these datas by using async function for faster. Once we get the data, we can push it into the list.`

5. Empty the survey_report table.
    ```sql
    TRUNCATE TABLE survey_report;
    ```

6. Insert all lists into survey_report.
    ```sql
    INSERT INTO survey_report (EmployeeId, FirstName, LastName, <survey_columns>)
    VALUES (<corresponding_values>);
    ```

7. Close the database connection.

    ```python
    cursor.close()
    conn.close()
    ```
