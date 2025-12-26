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
import numpy as np


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
    
    def fix_dataframe_structure(self, df):
        """Fix misaligned columns in the DataFrame"""
        print("    ⚙ Checking and fixing DataFrame structure...")
        
        # Expected columns from your data
        expected_columns = [
            'appointment_id', 'patient_id', 'doctor_id', 'appointment_date', 
            'appointment_time', 'status', 'reason', 'notes', 'suggest', 
            'fees', 'payment_method', 'discount', 'diagnosis'
        ]
        
        # If we have the right number of columns but wrong names (misalignment)
        if len(df.columns) == len(expected_columns):
            current_cols = list(df.columns)
            
            # Check if this is the problematic Appointment file
            if 'appointment_id' in current_cols and 'diagnosis' in current_cols:
                # Look for rows where 'suggest' column contains numeric values (should be in 'fees')
                problematic_rows = df[pd.to_numeric(df['suggest'], errors='coerce').notna()]
                
                if len(problematic_rows) > 0:
                    print(f"    ⚠ Found {len(problematic_rows)} misaligned rows. Fixing...")
                    
                    # Create a copy to avoid SettingWithCopyWarning
                    df_fixed = df.copy()
                    
                    # For each problematic row, shift columns starting from 'suggest' one position right
                    mask = pd.to_numeric(df_fixed['suggest'], errors='coerce').notna()
                    
                    # Store original values
                    suggest_vals = df_fixed.loc[mask, 'suggest'].copy()
                    fees_vals = df_fixed.loc[mask, 'fees'].copy()
                    payment_vals = df_fixed.loc[mask, 'payment_method'].copy()
                    discount_vals = df_fixed.loc[mask, 'discount'].copy()
                    diagnosis_vals = df_fixed.loc[mask, 'diagnosis'].copy()
                    
                    # Shift values: suggest gets NaN, fees gets suggest value, etc.
                    df_fixed.loc[mask, 'suggest'] = np.nan
                    df_fixed.loc[mask, 'fees'] = pd.to_numeric(suggest_vals, errors='coerce')
                    df_fixed.loc[mask, 'payment_method'] = fees_vals
                    df_fixed.loc[mask, 'discount'] = pd.to_numeric(payment_vals, errors='coerce')
                    
                    # Keep diagnosis as is since it's already correct
                    
                    print(f"    ✓ Fixed {len(problematic_rows)} misaligned rows")
                    return df_fixed
        
        return df
    
    def insert_dataframe(self, df, table_name):
        cursor = self.connection.cursor()
        
        # Debug: Show what's being inserted
        print(f"    DEBUG: DataFrame shape before cleaning: {df.shape}")
        if not df.empty:
            print(f"    DEBUG: First row values:")
            for col, val in zip(df.columns, df.iloc[0]):
                print(f"      {col}: {val} (type: {type(val)})")
        
        df_clean = df.copy()
        
        # Convert date and time columns properly
        date_cols = ['appointment_date']
        time_cols = ['appointment_time']
        
        for col in df_clean.columns:
            # Handle dates
            if col in date_cols:
                try:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                    df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')
                except:
                    pass
            
            # Handle times  
            elif col in time_cols:
                try:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                    df_clean[col] = df_clean[col].dt.strftime('%H:%M:%S')
                except:
                    pass
            
            # Handle other datetime columns
            elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Replace NaN with None for MySQL
            df_clean[col] = df_clean[col].where(pd.notna(df_clean[col]), None)
        
        # Debug: Show cleaned data
        print(f"    DEBUG: DataFrame shape after cleaning: {df_clean.shape}")
        if not df_clean.empty:
            print(f"    DEBUG: First row after cleaning:")
            for col, val in zip(df_clean.columns, df_clean.iloc[0]):
                print(f"      {col}: {val} (type: {type(val)})")
        
        # Prepare for insertion
        sanitized_cols = [self.sanitize_name(str(col)) for col in df_clean.columns]
        placeholders = ', '.join(['%s'] * len(sanitized_cols))
        columns_str = ', '.join([f"`{col}`" for col in sanitized_cols])
        
        insert_query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data = []
        for _, row in df_clean.iterrows():
            data.append(tuple(row))
        
        # Debug: Show first insert statement
        if data:
            print(f"    DEBUG: First INSERT statement would be:")
            print(f"      INSERT INTO `{table_name}` ({columns_str})")
            print(f"      VALUES {data[0]}")
        
        try:
            cursor.executemany(insert_query, data)
            self.connection.commit()
            print(f"  ✓ Inserted {len(df_clean)} rows into {table_name}")
        except Error as e:
            print(f"  ✗ Error inserting data: {e}")
            print(f"  First problematic row: {data[0] if data else 'No data'}")
            self.connection.rollback()
            raise
        
        cursor.close()
    
    def import_excel_file(self, file_path):
        try:
            file_name = Path(file_path).stem
            print(f"\n📊 Processing: {Path(file_path).name}")
            print(f"  Full path: {file_path}")
            
            # Try different Excel engines if needed
            try:
                excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            except:
                excel_file = pd.ExcelFile(file_path)
                
            sheets = excel_file.sheet_names
            print(f"  Found {len(sheets)} sheet(s): {sheets}")
            
            file_has_data = False
            
            for sheet_name in sheets:
                print(f"\n  ── Analyzing sheet: '{sheet_name}' ──")
                
                # Read the Excel file
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                except:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                # Debug information
                print(f"    DataFrame shape: {df.shape}")
                print(f"    DataFrame empty? {df.empty}")
                print(f"    Columns ({len(df.columns)}): {list(df.columns)}")
                
                # Show column dtypes
                print(f"    Column dtypes:")
                for col in df.columns:
                    print(f"      {col}: {df[col].dtype}")
                
                # Check if DataFrame has data
                if df.empty:
                    print(f"    ⚠ DataFrame is completely empty")
                    continue
                
                # Check for non-empty rows
                non_empty_rows = df.dropna(how='all').shape[0]
                if non_empty_rows == 0:
                    print(f"    ⚠ DataFrame has only NaN/empty values")
                    # Show raw data
                    print(f"    Raw data (first 3 rows):")
                    print(df.head(3).to_string())
                    continue
                
                print(f"    Rows with data: {non_empty_rows}/{df.shape[0]}")
                
                # Fix misaligned data (specifically for Appointment.xlsx)
                if file_name.lower() == 'appointment':
                    df = self.fix_dataframe_structure(df)
                
                # Show sample data
                print(f"    Sample data (first 3 rows):")
                print(df.head(3).to_string(index=False))
                
                # Now we have data
                file_has_data = True
                
                # Determine table name
                if len(sheets) > 1:
                    table_name = self.sanitize_name(f"{file_name}_{sheet_name}")
                else:
                    table_name = self.sanitize_name(file_name)
                
                print(f"    Creating table: {table_name}")
                
                try:
                    self.create_table_from_dataframe(df, table_name)
                    self.insert_dataframe(df, table_name)
                except Exception as e:
                    print(f"    ✗ Error processing table {table_name}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            # Final check
            if not file_has_data:
                print(f"\n  ⚠ File '{Path(file_path).name}' contains no usable data.")
                return None
            
            return True
            
        except Exception as e:
            print(f"  ✗ Error processing {Path(file_path).name}: {e}")
            import traceback
            traceback.print_exc()
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
        empty = 0
        
        for file_path in excel_files:
            result = self.import_excel_file(file_path)
            if result is True:
                successful += 1
            elif result is False:
                failed += 1
            else:  # result is None (empty file)
                empty += 1
        
        print(f"\n{'='*60}")
        print(f"IMPORT SUMMARY")
        print(f"{'='*60}")
        print(f"✓ Successfully imported: {successful} file(s)")
        print(f"○ Empty (skipped): {empty} file(s)")
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
                    
                    # Show table structure
                    cursor.execute(f"DESCRIBE `{table[0]}`")
                    columns = cursor.fetchall()
                    print(f"     Columns: {len(columns)}")
                    
                    # Show row count
                    cursor.execute(f"SELECT COUNT(*) FROM `{table[0]}`")
                    count = cursor.fetchone()[0]
                    print(f"     Rows: {count}")
                    
                    # Show first few rows
                    if count > 0:
                        cursor.execute(f"SELECT * FROM `{table[0]}` LIMIT 3")
                        sample_rows = cursor.fetchall()
                        cursor.execute(f"DESCRIBE `{table[0]}`")
                        column_names = [col[0] for col in cursor.fetchall()]
                        print(f"     Sample data (first 3 rows):")
                        for row in sample_rows:
                            print(f"       {dict(zip(column_names, row))}")
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