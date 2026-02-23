"""
Module for pulling data from Databricks SQL warehouse.
"""

from databricks import sql
import pandas as pd
from typing import Optional, List, Dict, Any


class DatabricksConnection:
    """Connect to and query Databricks SQL warehouse."""
    
    def __init__(
        self,
        hostname: str,
        http_path: str,
        personal_access_token: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ):
        """
        Initialize Databricks connection.
        
        Args:
            hostname: Databricks workspace hostname (e.g., 'adb-xxx.azuredatabricks.net')
            http_path: HTTP path for SQL warehouse (e.g., '/sql/1.0/warehouses/xxx')
            personal_access_token: Databricks personal access token
            catalog: Optional catalog name
            schema: Optional schema name
        """
        self.hostname = hostname
        self.http_path = http_path
        self.personal_access_token = personal_access_token
        self.catalog = catalog
        self.schema = schema
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to Databricks."""
        try:
            self.connection = sql.connect(
                host=self.hostname,
                http_path=self.http_path,
                auth_type="pat",
                token=self.personal_access_token
            )
            self.cursor = self.connection.cursor()
            print("✓ Connected to Databricks")
        except Exception as e:
            print(f"✗ Failed to connect to Databricks: {e}")
            raise
    
    def execute_query(self, query: str) -> List[tuple]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            
        Returns:
            List of result tuples
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print(f"✗ Query execution failed: {e}")
            raise
    
    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame containing query results
        """
        try:
            results = self.execute_query(query)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in self.cursor.description]
            
            # Create DataFrame
            df = pd.DataFrame(results, columns=columns)
            print(f"✓ Retrieved {len(df)} rows")
            return df
        except Exception as e:
            print(f"✗ Failed to convert results to DataFrame: {e}")
            raise
    
    def close(self):
        """Close the connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("✓ Connection closed")


def pull_databricks_data(
    hostname: str,
    http_path: str,
    personal_access_token: str,
    query: str,
    as_dataframe: bool = True
) -> Any:
    """
    Pull data from Databricks.
    
    Args:
        hostname: Databricks workspace hostname
        http_path: HTTP path for SQL warehouse
        personal_access_token: Databricks personal access token
        query: SQL query to execute
        as_dataframe: If True, return pandas DataFrame; if False, return list of tuples
        
    Returns:
        Query results as DataFrame or list of tuples
    """
    db = DatabricksConnection(hostname, http_path, personal_access_token)
    try:
        db.connect()
        if as_dataframe:
            return db.query_to_dataframe(query)
        else:
            return db.execute_query(query)
    finally:
        db.close()


if __name__ == "__main__":
    # Example usage
    # Set your Databricks credentials
    HOSTNAME = "your-workspace.azuredatabricks.net"
    HTTP_PATH = "/sql/1.0/warehouses/your-warehouse-id"
    TOKEN = "your-personal-access-token"
    
    # Example query
    QUERY = "SELECT * FROM your_catalog.your_schema.your_table LIMIT 100"
    
    # Pull data as DataFrame
    print("Pulling data from Databricks...")
    df = pull_databricks_data(
        hostname=HOSTNAME,
        http_path=HTTP_PATH,
        personal_access_token=TOKEN,
        query=QUERY
    )
    
    print("\nData preview:")
    print(df.head())
    print(f"\nShape: {df.shape}")
