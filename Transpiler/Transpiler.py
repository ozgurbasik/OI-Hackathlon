import os
import glob
import pandas as pd
import re
from pathlib import Path
import google.generativeai as genai
from datetime import datetime
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLToPySparkTranspiler:
    def __init__(self, api_key_path="Transpiler/Testing/api_key.txt"):
        """
        Initialize the transpiler with Gemini API key
        
        Args:
            api_key_path (str): Path to the API key file
        """
        self.model = None
        self.setup_gemini_api(api_key_path)
        
        # Define paths
        self.report_path = "Profiler/output/"
        self.sql_files_path = "Profiler/sql_test_files_v2/"
        self.output_path = "Transpiler/Transpiled_sql_files/"
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)
    
    def setup_gemini_api(self, api_key_path):
        """Setup Gemini AI API."""
        try:
            if not os.path.exists(api_key_path):
                logger.error(f"API key file not found at: {api_key_path}")
                return False
                
            with open(api_key_path, "r") as file:
                api_key = file.read().strip()
            
            # Configure the API key
            genai.configure(api_key=api_key)
            
            # Create the model
            self.model = genai.GenerativeModel("gemini-2.0-flash-exp")
            
            logger.info("Gemini AI API configured successfully")
            return True
        except Exception as e:
            logger.error(f"Error setting up Gemini API: {e}")
            return False
    
    def find_latest_complexity_report(self):
        """Find the latest SQL complexity report CSV file in Profiler/output/."""
        try:
            # Look for files in the correct directory
            pattern = os.path.join(self.report_path, "sql_complexity_report*.csv")
            files = glob.glob(pattern)
            
            if not files:
                logger.error(f"No SQL complexity report files found in {self.report_path}")
                return None
            
            # Sort files to get the latest one
            def extract_number(filename):
                basename = os.path.basename(filename)
                match = re.search(r'sql_complexity_report(?:_(\d+))?\.csv$', basename)
                if match:
                    return int(match.group(1)) if match.group(1) else 0
                return -1
            
            files.sort(key=extract_number, reverse=True)
            latest_file = files[0]
            
            logger.info(f"Found latest report: {latest_file}")
            return latest_file
        except Exception as e:
            logger.error(f"Error finding complexity report: {e}")
            return None
    
    def read_complexity_report(self, filename):
        """Read and parse the complexity report CSV."""
        try:
            df = pd.read_csv(filename)
            logger.info(f"Successfully loaded {len(df)} records from {filename}")
            
            # Validate required columns
            required_columns = ['filename', 'classification', 'lines_of_code', 'complexity_score']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return None
            
            return df
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")
            return None
    
    def get_sql_file_path(self, filename):
        """Get the full path to the SQL file."""
        file_path = os.path.join(self.sql_files_path, filename)
        
        if os.path.exists(file_path):
            logger.info(f"Found SQL file: {file_path}")
            return file_path
        else:
            logger.error(f"SQL file not found at: {file_path}")
            return None
    
    def read_sql_file(self, file_path):
        """Read the SQL file content."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            logger.info(f"Successfully read SQL file: {file_path}")
            return content
        except Exception as e:
            logger.error(f"Error reading SQL file: {e}")
            return None
    
    def extract_pyspark_code(self, response_text):
        """Extract clean PySpark code from Gemini response."""
        try:
            # Remove markdown code blocks if present
            code = response_text
            
            # Remove markdown code block markers
            if "```python" in code:
                code = re.sub(r'```python\n?', '', code)
                code = re.sub(r'\n?```', '', code)
            elif "```" in code:
                code = re.sub(r'```\n?', '', code)
            
            # Clean up any extra explanatory text at the beginning or end
            lines = code.split('\n')
            
            # Find the first import or actual code line
            start_idx = 0
            for i, line in enumerate(lines):
                stripped = line.strip()
                if (stripped.startswith(('from ', 'import ', 'def ', 'class ', '#')) or 
                    stripped and not stripped.startswith(('Here', 'This', 'The', 'Note'))):
                    start_idx = i
                    break
            
            # Take everything from the first code line onwards
            cleaned_code = '\n'.join(lines[start_idx:])
            
            return cleaned_code.strip()
        except Exception as e:
            logger.error(f"Error extracting PySpark code: {e}")
            return response_text
    
    def transpile_sql_to_pyspark(self, sql_content, filename):
        """Use Gemini AI to transpile SQL to PySpark."""
        try:
            logger.info(f"Transpiling {filename} using Gemini AI...")
            
            prompt = f"""
Convert the following SQL code to PySpark code. Requirements:

1. Use proper PySpark DataFrame API syntax
2. Import necessary PySpark modules at the top (from pyspark.sql import SparkSession, functions as F, types as T, Window)
3. Assume SparkSession is already created as 'spark'
4. Use descriptive variable names
5. Add comments explaining complex transformations
6. Handle JOINs, window functions, CTEs, and aggregations appropriately
7. Maintain the original logic and functionality
8. Format the code properly with proper indentation
9. Return ONLY the PySpark code, no explanations or markdown

Original SQL filename: {filename}

SQL Code:
{sql_content}
"""
            
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                # Extract clean PySpark code
                pyspark_code = self.extract_pyspark_code(response.text)
                logger.info("Successfully transpiled SQL to PySpark")
                return pyspark_code
            else:
                logger.error("No response received from Gemini AI")
                return None
                
        except Exception as e:
            logger.error(f"Error during transpilation: {e}")
            return None
    
    def save_pyspark_file(self, pyspark_code, original_filename, classification):
        """Save the transpiled PySpark code to file with classification in filename."""
        try:
            # Create output filename: filename_classification_pyspark.py
            base_name = os.path.splitext(original_filename)[0]
            output_filename = f"{base_name}_{classification.lower()}_pyspark.py"
            output_path = os.path.join(self.output_path, output_filename)
            
            # Add header with metadata
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            header = f'''"""
PySpark code transpiled from {original_filename}
Classification: {classification}
Generated on: {timestamp}
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

'''
            
            # Write the file
            with open(output_path, 'w', encoding='utf-8') as file:
                file.write(header + pyspark_code)
            
            logger.info(f"PySpark code saved to: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving PySpark file: {e}")
            return None
    
    def process_sql_file(self, filename, classification):
        """Process a single SQL file - read, transpile, and save."""
        logger.info(f"Processing: {filename} (Classification: {classification})")
        
        # Get SQL file path
        sql_path = self.get_sql_file_path(filename)
        if not sql_path:
            return False
        
        # Read SQL content
        sql_content = self.read_sql_file(sql_path)
        if not sql_content:
            return False
        
        logger.info(f"SQL file size: {len(sql_content)} characters")
        
        # Transpile to PySpark
        pyspark_code = self.transpile_sql_to_pyspark(sql_content, filename)
        if not pyspark_code:
            return False
        
        # Save PySpark file
        output_path = self.save_pyspark_file(pyspark_code, filename, classification)
        if output_path:
            logger.info(f"Successfully transpiled {filename} to PySpark!")
            return True
        
        return False
    
    def run_transpiler(self):
        """Main method to run the transpiler."""
        logger.info("Starting SQL to PySpark Transpiler")
        
        if not self.model:
            logger.error("Cannot proceed without Gemini AI API. Please check your API key.")
            return
        
        # Find and read the latest report
        report_file = self.find_latest_complexity_report()
        if not report_file:
            return
        
        df = self.read_complexity_report(report_file)
        if df is None:
            return
        
        # Process files from bottom to top (reverse order)
        logger.info(f"Processing {len(df)} files from bottom to top...")
        
        # Reverse the dataframe to process from bottom to top
        df_reversed = df.iloc[::-1].reset_index(drop=True)
        
        success_count = 0
        failure_count = 0
        
        for idx, row in df_reversed.iterrows():
            filename = row['filename']
            classification = row['classification']
            
            logger.info(f"Processing file {idx + 1}/{len(df_reversed)}: {filename}")
            
            try:
                if self.process_sql_file(filename, classification):
                    success_count += 1
                    logger.info(f"✅ Successfully processed {filename}")
                else:
                    failure_count += 1
                    logger.error(f"❌ Failed to process {filename}")
                
                # Add a small delay to avoid overwhelming the API
                time.sleep(1)
                
            except Exception as e:
                failure_count += 1
                logger.error(f"❌ Error processing {filename}: {e}")
        
        # Summary
        logger.info(f"""
Transpilation Summary:
=====================
Total files processed: {len(df_reversed)}
Successful: {success_count}
Failed: {failure_count}
Output directory: {self.output_path}
""")

def main():
    """Main function to run the transpiler."""
    print("🚀 SQL to PySpark Transpiler")
    print("=" * 50)
    
    try:
        # Initialize and run transpiler
        transpiler = SQLToPySparkTranspiler()
        transpiler.run_transpiler()
        
        print("\n🎉 Transpilation process completed!")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        logger.error(f"Unexpected error in main: {e}")

if __name__ == "__main__":
    main()