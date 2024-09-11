## Scientific Article Management System

This project develops a comprehensive system for managing scientific articles using DOIs (Digital Object Identifiers). Designed to handle large volumes of complex data, the system integrates several modern technologies to streamline the research process.

### **Project Overview**

In scientific research, managing and analyzing vast amounts of data efficiently is crucial. This system provides an infrastructure for processing and storing scientific articles from text files containing DOIs. By leveraging advanced tools and non-relational databases, it automates the extraction, analysis, and storage of article metadata, simplifying the research workflow.

### **Key Components**

- **MongoDB:** Serves as the primary database for storing article data in JSON format. MongoDB’s flexibility allows for complex data structures and efficient querying.
  
- **Neo4j:** A graph database used to map relationships between articles and authors. Neo4j facilitates advanced queries on collaborations and connections within the dataset.
  
- **Spark:** Used for distributed data processing, enabling in-memory computations for fast and scalable analysis. Spark handles data loaded from MongoDB and performs various analyses.

- **Python & JupyterLab:** The system's interface is developed in Python, with JupyterLab providing an interactive environment for data analysis and development.

### **Deployment**

The system is deployed using Docker, simulating a distributed environment with the following components:

- **MongoDB:** Two nodes for redundancy and availability.
- **Neo4j:** Manages relationships and graph-based queries.
- **Spark Cluster:** Runs on a simulated HDFS for distributed data processing.
- **JupyterLab:** Offers an interactive interface for development and analysis.

### **Functionalities**

- **Static Data Generation:** Creates CSV files with key information from articles using MongoDB queries.
- **Dynamic Data Generation:** Processes article corpora with Spark to analyze keyword frequencies and generate dynamic data points.
- **Query Support:** Handles both simple and complex queries, including author collaborations and keyword searches.

### **Future Work**

Future articles will explore:

- **Data Processing Architecture:** Detailed insights into Spark's role in data processing.
- **Storage Architecture:** In-depth examination of MongoDB and Neo4j integration.

Additional topics may include data visualization methods and machine learning models to further enhance data analysis.

### **Collaborators**

This project was developed in collaboration with Pablo Alonso, Matías Mussini, and Antonio Reviejo

