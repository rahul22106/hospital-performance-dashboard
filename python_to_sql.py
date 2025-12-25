#!/usr/bin/env python3
"""
Hospital Performance Dashboard - Excel to MySQL Importer
Imports all Excel files from the dataset folder into MySQL database.
Author: Rahul Kumar Mishra
Project: hospital-performance-dashboard
"""

import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from pathlib import Path
import re
import sys


class ExcelToMySQL:
    
    def __init__(self, host='localhost', user='root', password='', database='hospital_db'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                print(f"✓ Connected to MySQL Server version {db_info}")
                self._create_database()
                return True
        except Error as e:
            print(f"✗ Error connecting to MySQL: {e}")
            print("\nPlease check:")
            print("  1. MySQL server is running")
            print("  2. Username and password are correct")
            print("  3. MySQL service is accessible on localhost")
            return False
    
    def _create_database(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE {self.database}")
            print(f"✓ Using database: {self.database}")
            cursor.close()
        except Error as e:
            print(f"✗ Error creating database: {e}")
            raise
    
    def sanitize_name(self, name):
        name = name.replace(' ', '_')
        name = re.sub(r'[^\w$]', '_', name)
        name = name.strip('_')
        if name and name[0].isdigit():
            name = '_' + name
        return name[:64]
    
    def get_sql_type(self, dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            return 'DOUBLE'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'DATETIME'
        else:
            return 'TEXT'
    
    def create_table_from_dataframe(self, df, table_name):
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        columns = []
        for col in df.columns:
            col_name = self.sanitize_name(str(col))
            col_type = self.get_sql_type(df[col].dtype)
            columns.append(f"`{col_name}` {col_type}")
        
        create_table_query = f"""
        CREATE TABLE `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        cursor.execute(create_table_query)
        cursor.close()
        print(f"  ✓ Created table: {table_name}")
    
    def insert_dataframe(self, df, table_name):
        cursor = self.connection.cursor()
        
        df_clean = df.copy()
        for col in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
        
        sanitized_cols = [self.sanitize_name(str(col)) for col in df.columns]
        placeholders = ', '.join(['%s'] * len(sanitized_cols))
        columns_str = ', '.join([f"`{col}`" for col in sanitized_cols])
        
        insert_query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        data = [tuple(row) for row in df_clean.values]
        
        cursor.executemany(insert_query, data)
        self.connection.commit()
        
        cursor.close()
        print(f"  ✓ Inserted {len(df)} rows into {table_name}")
    
    def import_excel_file(self, file_path):
        try:
            file_name = Path(file_path).stem
            print(f"\n📊 Processing: {Path(file_path).name}")
            
            excel_file = pd.ExcelFile(file_path)
            sheets = excel_file.sheet_names
            print(f"  Found {len(sheets)} sheet(s)")
            
            for sheet_name in sheets:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                if df.empty:
                    print(f"  ⚠ Skipping empty sheet: {sheet_name}")
                    continue
                
                if len(sheets) > 1:
                    table_name = self.sanitize_name(f"{file_name}_{sheet_name}")
                else:
                    table_name = self.sanitize_name(file_name)
                
                self.create_table_from_dataframe(df, table_name)
                self.insert_dataframe(df, table_name)
            
            return True
            
        except Exception as e:
            print(f"  ✗ Error processing {Path(file_path).name}: {e}")
            return False
    
    def import_folder(self, folder_path):
        folder = Path(folder_path)
        
        if not folder.exists():
            print(f"✗ Folder not found: {folder_path}")
            print(f"  Please create the folder and add your Excel files")
            return
        
        excel_files = list(folder.glob('*.xlsx')) + list(folder.glob('*.xls'))
        
        if not excel_files:
            print(f"✗ No Excel files found in: {folder_path}")
            print(f"  Please add .xlsx or .xls files to the folder")
            return
        
        print(f"\n📁 Found {len(excel_files)} Excel file(s) in '{folder_path}'")
        
        successful = 0
        failed = 0
        
        for file_path in excel_files:
            if self.import_excel_file(file_path):
                successful += 1
            else:
                failed += 1
        
        print(f"\n{'='*60}")
        print(f"IMPORT SUMMARY")
        print(f"{'='*60}")
        print(f"✓ Successfully imported: {successful} file(s)")
        if failed > 0:
            print(f"✗ Failed: {failed} file(s)")
        print(f"{'='*60}\n")
    
    def list_tables(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            if tables:
                print(f"\n📋 Tables in database '{self.database}':")
                for i, table in enumerate(tables, 1):
                    print(f"  {i}. {table[0]}")
            else:
                print(f"\n📋 No tables found in database '{self.database}'")
            
            cursor.close()
        except Error as e:
            print(f"✗ Error listing tables: {e}")
    
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("✓ MySQL connection closed")


def main():
    print("\n" + "="*60)
    print("HOSPITAL PERFORMANCE DASHBOARD - Excel to MySQL Importer")
    print("="*60)
    
    print("\n📝 MySQL Configuration:")
    MYSQL_CONFIG = {
        'host': input("  MySQL Host [localhost]: ").strip() or 'localhost',
        'user': input("  MySQL User [root]: ").strip() or 'root',
        'password': input("  MySQL Password: ").strip(),
        'database': input("  Database Name [hospital_db]: ").strip() or 'hospital_db'
    }
    
    DATASET_FOLDER = input("\n📁 Dataset Folder [dataset]: ").strip() or 'dataset'
    
    print("\n" + "="*60)
    
    importer = ExcelToMySQL(**MYSQL_CONFIG)
    
    if importer.connect():
        importer.import_folder(DATASET_FOLDER)
        importer.list_tables()
        importer.close()
        
        print("\n✅ Import completed successfully!")
        print(f"   You can now view your data in MySQL Workbench")
        print(f"   Database: {MYSQL_CONFIG['database']}")
    else:
        print("\n✗ Failed to connect to MySQL. Please check your configuration.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Import cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)