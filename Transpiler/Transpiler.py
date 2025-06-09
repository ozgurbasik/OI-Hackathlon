import os
import glob
import pandas as pd
import re
from pathlib import Path
import google.generativeai as genai
from datetime import datetime

def find_latest_report():
    """Find the latest SQL complexity report CSV file."""
    # Look for files matching the pattern
    pattern = "sql_complexity_report*.csv"
    files = glob.glob(pattern)
    
    if not files:
        print("❌ No SQL complexity report files found!")
        return None
    
    # Sort files to get the latest one
    # Handle both sql_complexity_report.csv and sql_complexity_report_N.csv
    def extract_number(filename):
        match = re.search(r'sql_complexity_report(?:_(\d+))?\.csv$', filename)
        if match:
            return int(match.group(1)) if match.group(1) else 0
        return -1
    
    files.sort(key=extract_number, reverse=True)
    latest_file = files[0]
    
    print(f"📊 Found latest report: {latest_file}")
    return latest_file

def read_complexity_report(filename):
    """Read and parse the complexity report CSV."""
    try:
        df = pd.read_csv(filename)
        print(f"✅ Successfully loaded {len(df)} records from {filename}")
        return df
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return None

def display_files_by_complexity(df):
    """Display files grouped by complexity level."""
    complexities = ['Complex', 'Medium', 'Simple']
    
    for complexity in complexities:
        files = df[df['classification'] == complexity]
        print(f"\n{'='*50}")
        print(f"🔍 {complexity.upper()} FILES ({len(files)} files)")
        print(f"{'='*50}")
        
        if len(files) == 0:
            print("   No files found in this category")
            continue
            
        # Sort by complexity score (descending) then by lines of code
        files_sorted = files.sort_values(['complexity_score', 'lines_of_code'], ascending=[False, False])
        
        for idx, (_, row) in enumerate(files_sorted.iterrows(), 1):
            print(f"{idx:2d}. {row['filename']:<20} | LOC: {row['lines_of_code']:>3d} | "
                  f"Complexity: {row['complexity_score']:>3d} | {row['reason']}")

def get_user_selection(df):
    """Get user input for file selection."""
    while True:
        print(f"\n{'='*60}")
        print("📝 SELECT A FILE TO PROCESS")
        print(f"{'='*60}")
        
        choice = input("\nEnter filename (e.g., 'query_049.sql') or 'exit' to quit: ").strip()
        
        if choice.lower() in ['exit', 'quit', 'q']:
            print("👋 Goodbye!")
            return None
            
        # Check if file exists in the report
        matching_files = df[df['filename'] == choice]
        
        if len(matching_files) == 0:
            print(f"❌ File '{choice}' not found in the report. Please try again.")
            continue
            
        return choice

def get_sql_file_path(filename):
    """Get the full path to the SQL file."""
    sql_folder = "Profiler/sql_test_files_v2"
    file_path = os.path.join(sql_folder, filename)
    
    if os.path.exists(file_path):
        print(f"✅ Found SQL file: {file_path}")
        return file_path
    else:
        print(f"❌ SQL file not found at: {file_path}")
        return None

def setup_gemini_api():
    """Setup Gemini AI API."""
    try:
        # Read API key from file
        api_key_path = "Transpiler/Testing/api_key.txt"
        if not os.path.exists(api_key_path):
            print(f"❌ API key file not found at: {api_key_path}")
            return None
            
        with open(api_key_path, "r") as file:
            api_key = file.read().strip()
        
        # Configure the API key
        genai.configure(api_key=api_key)
        
        # Create the model
        model = genai.GenerativeModel("gemini-2.0-flash-exp")
        
        print("✅ Gemini AI API configured successfully")
        return model
    except Exception as e:
        print(f"❌ Error setting up Gemini API: {e}")
        return None

def read_sql_file(file_path):
    """Read the SQL file content."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        print(f"✅ Successfully read SQL file: {file_path}")
        return content
    except Exception as e:
        print(f"❌ Error reading SQL file: {e}")
        return None

def transpile_sql_to_pyspark(model, sql_content, filename):
    """Use Gemini AI to transpile SQL to PySpark."""
    try:
        print(f"🤖 Transpiling {filename} using Gemini AI...")
        
        prompt = f"""
Please convert the following SQL code to PySpark code. 
Follow these guidelines:

1. Use proper PySpark DataFrame API syntax
2. Import necessary PySpark modules at the top
3. Assume SparkSession is already created as 'spark'
4. Use descriptive variable names
5. Add comments explaining complex transformations
6. Handle JOINs, window functions, CTEs, and aggregations appropriately
7. Maintain the original logic and functionality
8. Format the code properly with proper indentation

Original SQL filename: {filename}

SQL Code:
```sql
{sql_content}
```

Please provide only the PySpark code without additional explanations.
"""
        
        response = model.generate_content(prompt)
        
        if response and response.text:
            print("✅ Successfully transpiled SQL to PySpark")
            return response.text
        else:
            print("❌ No response received from Gemini AI")
            return None
            
    except Exception as e:
        print(f"❌ Error during transpilation: {e}")
        return None

def save_pyspark_file(pyspark_code, original_filename):
    """Save the transpiled PySpark code to file."""
    try:
        # Create output directory if it doesn't exist
        output_dir = "Transpiler/Transpiled_sql_files"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create output filename
        base_name = os.path.splitext(original_filename)[0]
        output_filename = f"{base_name}_pyspark.py"
        output_path = os.path.join(output_dir, output_filename)
        
        # Add header with metadata
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f'''"""
PySpark code transpiled from {original_filename}
Generated on: {timestamp}
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

'''
        
        # Write the file
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(header + pyspark_code)
        
        print(f"✅ PySpark code saved to: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"❌ Error saving PySpark file: {e}")
        return None

def process_sql_file(model, df, filename):
    """Process the selected SQL file - read, transpile, and save."""
    print(f"\n{'='*60}")
    print(f"🔄 PROCESSING: {filename}")
    print(f"{'='*60}")
    
    # Display file information first
    display_file_info(df, filename)
    
    # Get SQL file path
    sql_path = get_sql_file_path(filename)
    if not sql_path:
        return False
    
    # Read SQL content
    sql_content = read_sql_file(sql_path)
    if not sql_content:
        return False
    
    print(f"📄 SQL file size: {len(sql_content)} characters")
    
    # Transpile to PySpark
    pyspark_code = transpile_sql_to_pyspark(model, sql_content, filename)
    if not pyspark_code:
        return False
    
    # Save PySpark file
    output_path = save_pyspark_file(pyspark_code, filename)
    if output_path:
        print(f"🎉 Successfully transpiled {filename} to PySpark!")
        return True
    
    return False

def display_file_info(df, filename):
    """Display detailed information about the selected file."""
    file_info = df[df['filename'] == filename].iloc[0]
    
    print(f"\n{'='*60}")
    print(f"📄 FILE INFORMATION: {filename}")
    print(f"{'='*60}")
    print(f"Classification:      {file_info['classification']}")
    print(f"Lines of Code:       {file_info['lines_of_code']}")
    print(f"Complexity Score:    {file_info['complexity_score']}")
    print(f"Reason:              {file_info['reason']}")
    print(f"Score Detail:        {file_info['score_detail']}")
    
    # Display complexity breakdown
    complexity_features = [
        ('Joins', 'joins'),
        ('CTEs', 'ctes'),
        ('Subqueries', 'subqueries'),
        ('Window Functions', 'window_functions'),
        ('Unions', 'unions'),
        ('Case Statements', 'case_statements'),
        ('Aggregates', 'aggregates'),
        ('Having Clauses', 'having_clauses'),
        ('Exists Clauses', 'exists_clauses'),
        ('Recursive CTEs', 'recursive_ctes'),
        ('Pivots', 'pivots'),
        ('Dynamic SQL', 'dynamic_sql')
    ]
    
    print(f"\n📊 COMPLEXITY BREAKDOWN:")
    print(f"{'-'*40}")
    for feature_name, column in complexity_features:
        value = file_info[column]
        if value > 0:
            print(f"{feature_name:<18}: {value}")

def main():
    """Main function to orchestrate the script."""
    print("🚀 SQL Complexity Report Reader & PySpark Transpiler")
    print("=" * 60)
    
    # Setup Gemini AI
    model = setup_gemini_api()
    if not model:
        print("❌ Cannot proceed without Gemini AI API. Please check your API key.")
        return
    
    # Find and read the latest report
    report_file = find_latest_report()
    if not report_file:
        return
    
    df = read_complexity_report(report_file)
    if df is None:
        return
    
    # Display files by complexity
    display_files_by_complexity(df)
    
    # Get user selection and process files
    while True:
        selected_file = get_user_selection(df)
        if selected_file is None:
            break
        
        # Process the selected file
        success = process_sql_file(model, df, selected_file)
        
        if success:
            print(f"\n✨ {selected_file} has been successfully transpiled to PySpark!")
        else:
            print(f"\n❌ Failed to transpile {selected_file}")
        
        # Ask if user wants to select another file
        another = input("\nSelect another file to transpile? (y/n): ").strip().lower()
        if another not in ['y', 'yes']:
            print("👋 Thank you for using SQL to PySpark Transpiler!")
            break

if __name__ == "__main__":
    main()