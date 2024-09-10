<h1> Globant Data Engineering Challenge </h1>

Welcome to the solution for the Globant Data Engineering Challenge. This project demonstrates a data pipeline that extracts data from a MySQL database, processes it using Python, and loads it into a Snowflake database. Below is a detailed explanation of the solution and how to test it.

**Architecture**

The solution follows a standard ETL (Extract, Transform, Load) workflow and is implemented with the following components:

1. Source Extraction: Data is extracted from a MySQL database hosted on Railway.

2. Processing: Data processing is handled by a Python application deployed on Render.

3. Target Loading: Processed data is loaded into a Snowflake database.

The architecture is visualized in the globchal_diagram.pdf file, which provides a detailed overview of the data flow.

**Getting Started**
**Prerequisites**

Before testing the application, ensure you have the following:

1. Access to Postman or a similar API testing tool.

2. Proper configuration values for MySQL and Snowflake.

**Testing the Application**
To test the application, follow these steps:

1. Send a POST Request: Use Postman or any HTTP client to send a POST request to one of the following URLs. Both URLs provide the same functionality, with the first one using a GitHub-hosted application and the second one using a Docker image deployed on Render:

GitHub-hosted: https://globchal-1.onrender.com/execute

Docker image on Render: https://globchal-rep-docker.onrender.com/execute

2. Configure the Request Body: In Postman, select the Body tab, choose the Raw option, and set the format to JSON. Then, enter the following JSON body:

{
  "configuration": {
    "sourceConnection": {
      "username": "username_value",
      "password": "password_value",
      "host": "host_value",
      "port": "port_value",
      "database": "database_value",
      "connectorName": "MySQL"
    },
    "targetConnection": {
      "username": "username_value",
      "password": "password_value",
      "account": "account_value",
      "warehouse": "warehouse_value",
      "database": "database_value",
      "schema": "schema_value",
      "databaseRole": "databaseRole_value",
      "connectorName": "Snowflake"
    }
  },
  "chunk_size": "chunk_size_value"
}

Replace the placeholder values with your actual MySQL and Snowflake connection details.

**3. Send the Request**

Click Send in Postman to execute the request. The application will process the data and load it into Snowflake according to the provided configuration.

**Deployment**

The application is containerized using Docker and deployed on Render. You can view the deployment status and logs on the Render dashboard.

**Additional Information**
Repository: GitHub Repository URL (Replace with your actual repository URL)
Diagram: globchal_diagram.pdf (Provides a visual representation of the architecture)
Docker Image: Available on Docker Hub (Replace with your actual Docker Hub repository if applicable)
