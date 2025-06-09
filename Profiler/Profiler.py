import re
import csv
import io
import logging
from typing import Dict, List, Tuple, Optional
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLComplexityAnalyzer:
    """Analyzes SQL scripts for complexity metrics and generates reports."""
    
    def __init__(self, account_name: str):
        self.account_name = account_name
        self.service_client = self._get_adls_client()
        
    def _get_adls_client(self) -> DataLakeServiceClient:
        """Initialize ADLS client with proper authentication."""
        try:
            credential = DefaultAzureCredential()
            account_url = f"https://{self.account_name}.dfs.core.windows.net"
            service_client = DataLakeServiceClient(
                account_url=account_url, 
                credential=credential
            )
            logger.info(f"Connected to ADLS account: {self.account_name}")
            return service_client
        except Exception as e:
            logger.error(f"Failed to connect to ADLS: {str(e)}")
            raise
    
    def read_sql_files(self, container_name: str, directory_path: str) -> Dict[str, str]:
        """Read all SQL files from specified container and directory."""
        sql_files = {}
        try:
            filesystem_client = self.service_client.get_file_system_client(container_name)
            paths = filesystem_client.get_paths(path=directory_path)
            
            for path in paths:
                if path.name.endswith('.sql'):
                    try:
                        file_client = filesystem_client.get_file_client(path.name)
                        file_contents = file_client.download_file().readall().decode('utf-8')
                        sql_files[path.name] = file_contents
                        logger.info(f"Read SQL file: {path.name}")
                    except Exception as e:
                        logger.warning(f"Failed to read file {path.name}: {str(e)}")
                        
        except AzureError as e:
            logger.error(f"Azure error while reading files: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while reading files: {str(e)}")
            raise
            
        logger.info(f"Successfully read {len(sql_files)} SQL files")
        return sql_files
    
    def get_complexity_score(self, sql_text: str) -> Tuple[int, int, Dict[str, int]]:
        """Calculate complexity score based on various SQL patterns."""
        if not sql_text or not sql_text.strip():
            return 0, 0, {}
            
        text = sql_text.lower()
        score = 0
        pattern_counts = {}
        
        # Enhanced complexity patterns with weights
        patterns = {
            'join': (r'\b(?:inner\s+join|left\s+join|right\s+join|full\s+join|cross\s+join|join)\b', 2),
            'cte': (r'\bwith\b\s+[a-zA-Z_][a-zA-Z0-9_]*\s+as\s*\(', 3),
            'subquery': (r'\(\s*select\b', 2),
            'window_function': (r'\bover\s*\([^)]*\)', 3),
            'union': (r'\bunion\s+(?:all\s+)?select\b', 2),
            'case_when': (r'\bcase\b.*?\bwhen\b', 1),
            'aggregate': (r'\b(?:sum|count|avg|min|max|group_concat)\s*\(', 1),
            'having': (r'\bhaving\b', 2),
            'exists': (r'\b(?:exists|not\s+exists)\b', 2),
            'recursive': (r'\bwith\s+recursive\b', 4),
            'pivot': (r'\bpivot\b', 3),
            'dynamic_sql': (r'exec\s*\(|execute\s*\(|sp_executesql', 4),
        }
        
        for pattern_name, (pattern, weight) in patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
            count = len(matches)
            pattern_counts[pattern_name] = count
            score += count * weight
        
        # Calculate lines of code (excluding empty lines and comments)
        lines = [line.strip() for line in sql_text.splitlines()]
        loc = len([line for line in lines if line and not line.startswith('--')])
        
        return score, loc, pattern_counts
    
    def classify_script(self, score: int, loc: int) -> Tuple[str, str, str]:
        """
        Classify script complexity based on the specified table:
        - Simple: 1-50 lines (Short scripts, basic SELECTs, filters, few or no joins)
        - Medium: 51-500 lines (Moderate logic, joins, CTEs, CASE, some nesting)
        - Complex: >500 lines (Large scripts, multiple CTEs, subqueries, window functions)
        
        Also considers complexity score for edge cases.
        """
        
        # Primary classification based on LOC as per the table
        if loc <= 50:
            primary_classification = 'Simple'
            base_reason = 'Short script (≤50 LOC)'
        elif loc <= 500:
            primary_classification = 'Medium'
            base_reason = 'Moderate length script (51-500 LOC)'
        else:
            primary_classification = 'Complex'
            base_reason = 'Large script (>500 LOC)'
        
        # Adjust classification based on complexity score if there's a significant mismatch
        complexity_indicators = []
        
        # High complexity score might bump up classification
        if score >= 30 and primary_classification == 'Simple':
            primary_classification = 'Medium'
            complexity_indicators.append('high complexity patterns despite low LOC')
        elif score >= 50 and primary_classification == 'Medium':
            primary_classification = 'Complex'
            complexity_indicators.append('very high complexity patterns')
        
        # Very low complexity score might suggest simpler classification for edge cases
        elif score <= 2 and primary_classification == 'Medium' and loc < 100:
            # Keep Medium but note the simplicity
            complexity_indicators.append('simple patterns despite moderate LOC')
        elif score <= 5 and primary_classification == 'Complex' and loc < 600:
            # This is an edge case - large but simple script
            complexity_indicators.append('simple patterns despite large LOC')
        
        # Build detailed reason
        reason_parts = [base_reason]
        
        if complexity_indicators:
            reason_parts.extend(complexity_indicators)
        
        # Add specific complexity details
        if score > 0:
            complexity_details = []
            if score >= 20:
                complexity_details.append('many complex patterns')
            elif score >= 10:
                complexity_details.append('moderate complex patterns')
            elif score >= 5:
                complexity_details.append('some complex patterns')
            else:
                complexity_details.append('few complex patterns')
            
            if complexity_details:
                reason_parts.append(f"with {complexity_details[0]}")
        
        reason = ', '.join(reason_parts)
        score_detail = f"LOC={loc}, Complexity={score}"
        
        return primary_classification, score_detail, reason
    
    def analyze_sql_scripts(self, container_name: str, directory_path: str) -> List[Dict]:
        """Analyze all SQL scripts and return complexity metrics."""
        try:
            sql_files = self.read_sql_files(container_name, directory_path)
            
            if not sql_files:
                logger.warning("No SQL files found to analyze")
                return []
            
            results = []
            
            for filename, content in sql_files.items():
                try:
                    score, loc, pattern_counts = self.get_complexity_score(content)
                    classification, score_detail, reason = self.classify_script(score, loc)
                    
                    result = {
                        'filename': filename,
                        'lines_of_code': loc,
                        'complexity_score': score,
                        'classification': classification,
                        'score_detail': score_detail,
                        'reason': reason,
                        'joins': pattern_counts.get('join', 0),
                        'ctes': pattern_counts.get('cte', 0),
                        'subqueries': pattern_counts.get('subquery', 0),
                        'window_functions': pattern_counts.get('window_function', 0),
                        'unions': pattern_counts.get('union', 0),
                        'case_statements': pattern_counts.get('case_when', 0),
                        'aggregates': pattern_counts.get('aggregate', 0),
                        'having_clauses': pattern_counts.get('having', 0),
                        'exists_clauses': pattern_counts.get('exists', 0),
                        'recursive_ctes': pattern_counts.get('recursive', 0),
                        'pivots': pattern_counts.get('pivot', 0),
                        'dynamic_sql': pattern_counts.get('dynamic_sql', 0)
                    }
                    
                    results.append(result)
                    logger.info(f"Analyzed {filename}: {classification} ({score_detail})")
                    
                except Exception as e:
                    logger.error(f"Failed to analyze {filename}: {str(e)}")
                    continue
            
            # Sort by complexity score descending
            results.sort(key=lambda x: x['complexity_score'], reverse=True)
            logger.info(f"Successfully analyzed {len(results)} SQL scripts")
            
            return results
            
        except Exception as e:
            logger.error(f"Error during SQL analysis: {str(e)}")
            raise
    
    def results_to_csv(self, results: List[Dict]) -> str:
        """Convert results to CSV format."""
        if not results:
            return ""
            
        output = io.StringIO()
        fieldnames = results[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
        
        csv_content = output.getvalue()
        logger.info(f"Generated CSV with {len(results)} records")
        return csv_content
    
    def upload_csv_to_adls(self, container_name: str, target_path: str, csv_content: str) -> bool:
        """Upload CSV content to ADLS with automatic filename versioning."""
        try:
            if not csv_content:
                logger.warning("No CSV content to upload")
                return False
            
            filesystem_client = self.service_client.get_file_system_client(container_name)
            
            # Check if file exists and get next available name
            actual_target_path = self._get_next_available_adls_path(filesystem_client, target_path)
            
            file_client = filesystem_client.get_file_client(actual_target_path)
            
            # Create the file
            file_client.create_file(overwrite=False)  # Don't overwrite since we found available name
            
            # Upload content
            csv_bytes = csv_content.encode('utf-8')
            file_client.append_data(data=csv_bytes, offset=0, length=len(csv_bytes))
            file_client.flush_data(len(csv_bytes))
            
            logger.info(f"Successfully uploaded CSV to: {actual_target_path}")
            return True
            
        except AzureError as e:
            logger.error(f"Azure error during upload: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during upload: {str(e)}")
            return False
    
    def _get_next_available_adls_path(self, filesystem_client, target_path: str) -> str:
        """Get next available path in ADLS if the target path already exists."""
        try:
            # Try to get properties of the file (this will raise exception if file doesn't exist)
            file_client = filesystem_client.get_file_client(target_path)
            file_client.get_file_properties()
            
            # If we reach here, file exists, so we need to find next available name
            logger.info(f"File {target_path} exists in ADLS, finding next available name...")
            
            # Split path into directory, name, and extension
            path_parts = target_path.split('/')
            filename = path_parts[-1]
            directory = '/'.join(path_parts[:-1]) if len(path_parts) > 1 else ''
            
            name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
            
            counter = 1
            while True:
                new_filename = f"{name}_{counter}.{ext}" if ext else f"{name}_{counter}"
                new_path = f"{directory}/{new_filename}" if directory else new_filename
                
                try:
                    # Check if this path exists
                    test_file_client = filesystem_client.get_file_client(new_path)
                    test_file_client.get_file_properties()
                    # If no exception, file exists, try next number
                    counter += 1
                except:
                    # File doesn't exist, we can use this path
                    logger.info(f"Using available path: {new_path}")
                    return new_path
                
                # Safety check
                if counter > 1000:
                    raise Exception("Too many existing files with similar names in ADLS")
                    
        except:
            # File doesn't exist, we can use the original path
            return target_path
    
    def generate_summary_report(self, results: List[Dict]) -> str:
        """Generate a summary of the analysis results."""
        if not results:
            return "No SQL files analyzed."
        
        total_files = len(results)
        classifications = {}
        total_loc = 0
        total_complexity = 0
        
        for result in results:
            classification = result['classification']
            classifications[classification] = classifications.get(classification, 0) + 1
            total_loc += result['lines_of_code']
            total_complexity += result['complexity_score']
        
        avg_loc = total_loc / total_files
        avg_complexity = total_complexity / total_files
        
        summary = f"""
SQL Complexity Analysis Summary
==============================
Total Files: {total_files}
Total Lines of Code: {total_loc:,}
Average LOC per file: {avg_loc:.1f}
Average Complexity Score: {avg_complexity:.1f}

Classification Distribution (Based on LOC thresholds):
  Simple (1-50 LOC): Short scripts, basic SELECTs, filters, few joins
  Medium (51-500 LOC): Moderate logic, joins, CTEs, CASE, some nesting  
  Complex (>500 LOC): Large scripts, multiple CTEs, subqueries, window functions
"""
        
        # Sort classifications in logical order
        classification_order = ['Simple', 'Medium', 'Complex']
        for classification in classification_order:
            if classification in classifications:
                count = classifications[classification]
                percentage = (count / total_files) * 100
                summary += f"  {classification}: {count} files ({percentage:.1f}%)\n"
        
        # Add any other classifications that might exist
        for classification, count in classifications.items():
            if classification not in classification_order:
                percentage = (count / total_files) * 100
                summary += f"  {classification}: {count} files ({percentage:.1f}%)\n"
        
        # Top 5 most complex files
        top_complex = sorted(results, key=lambda x: x['complexity_score'], reverse=True)[:5]
        summary += f"\nTop 5 Most Complex Files (by complexity score):\n"
        for i, file_info in enumerate(top_complex, 1):
            summary += f"  {i}. {file_info['filename']} (Score: {file_info['complexity_score']}, LOC: {file_info['lines_of_code']}, Class: {file_info['classification']})\n"
        
        # Largest files by LOC
        top_large = sorted(results, key=lambda x: x['lines_of_code'], reverse=True)[:5]
        summary += f"\nTop 5 Largest Files (by LOC):\n"
        for i, file_info in enumerate(top_large, 1):
            summary += f"  {i}. {file_info['filename']} (LOC: {file_info['lines_of_code']}, Score: {file_info['complexity_score']}, Class: {file_info['classification']})\n"
        
        return summary

def analyze_local_files(directory_path: str) -> List[Dict]:
    """Analyze SQL files from local directory without Azure connection."""
    import os
    
    results = []
    sql_files = {}
    
    # Read local SQL files
    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            relative_path = os.path.relpath(file_path, directory_path)
                            sql_files[relative_path] = content
                            logger.info(f"Read local SQL file: {relative_path}")
                    except Exception as e:
                        logger.warning(f"Failed to read file {file_path}: {str(e)}")
    except Exception as e:
        logger.error(f"Error reading local directory: {str(e)}")
        return []
    
    if not sql_files:
        logger.warning("No SQL files found in local directory")
        return []
    
    # Analyze files (reuse complexity analysis logic)
    analyzer_instance = SQLComplexityAnalyzer.__new__(SQLComplexityAnalyzer)  # Create instance without Azure connection
    
    for filename, content in sql_files.items():
        try:
            score, loc, pattern_counts = analyzer_instance.get_complexity_score(content)
            classification, score_detail, reason = analyzer_instance.classify_script(score, loc)
            
            result = {
                'filename': filename,
                'lines_of_code': loc,
                'complexity_score': score,
                'classification': classification,
                'score_detail': score_detail,
                'reason': reason,
                'joins': pattern_counts.get('join', 0),
                'ctes': pattern_counts.get('cte', 0),
                'subqueries': pattern_counts.get('subquery', 0),
                'window_functions': pattern_counts.get('window_function', 0),
                'unions': pattern_counts.get('union', 0),
                'case_statements': pattern_counts.get('case_when', 0),
                'aggregates': pattern_counts.get('aggregate', 0),
                'having_clauses': pattern_counts.get('having', 0),
                'exists_clauses': pattern_counts.get('exists', 0),
                'recursive_ctes': pattern_counts.get('recursive', 0),
                'pivots': pattern_counts.get('pivot', 0),
                'dynamic_sql': pattern_counts.get('dynamic_sql', 0)
            }
            
            results.append(result)
            logger.info(f"Analyzed {filename}: {classification} ({score_detail})")
            
        except Exception as e:
            logger.error(f"Failed to analyze {filename}: {str(e)}")
            continue
    
    # Sort by complexity score descending
    results.sort(key=lambda x: x['complexity_score'], reverse=True)
    logger.info(f"Successfully analyzed {len(results)} SQL scripts")
    
    return results

def get_next_available_filename(base_path: str) -> str:
    """Get next available filename if the base path already exists."""
    import os
    
    if not os.path.exists(base_path):
        return base_path
    
    # Split the path into directory, name, and extension
    directory = os.path.dirname(base_path)
    filename = os.path.basename(base_path)
    name, ext = os.path.splitext(filename)
    
    counter = 1
    while True:
        new_filename = f"{name}_{counter}{ext}"
        new_path = os.path.join(directory, new_filename)
        
        if not os.path.exists(new_path):
            logger.info(f"File {base_path} exists, using {new_path} instead")
            return new_path
        
        counter += 1
        
        # Safety check to prevent infinite loop
        if counter > 1000:
            raise Exception("Too many existing files with similar names")

def save_local_csv(results: List[Dict], output_path: str) -> Tuple[bool, str]:
    """Save results to local CSV file. Returns success status and actual path used."""
    try:
        if not results:
            logger.warning("No results to save")
            return False, output_path
        
        # Create directory if it doesn't exist
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Get next available filename if original exists
        actual_output_path = get_next_available_filename(output_path)
        
        # Generate CSV content
        analyzer_instance = SQLComplexityAnalyzer.__new__(SQLComplexityAnalyzer)
        csv_content = analyzer_instance.results_to_csv(results)
        
        # Write to file
        with open(actual_output_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        logger.info(f"Successfully saved CSV to: {actual_output_path}")
        return True, actual_output_path
        
    except Exception as e:
        logger.error(f"Failed to save CSV file: {str(e)}")
        return False, output_path

def connect_and_upload_to_azure(results: List[Dict], account_name: str, container_name: str, 
                               input_path: str, output_path: str) -> bool:
    """Connect to Azure and upload results after analysis is complete."""
    try:
        print("\n" + "="*50)
        print("CONNECTING TO AZURE...")
        print("="*50)
        
        # Initialize analyzer with Azure connection
        analyzer = SQLComplexityAnalyzer(account_name)
        
        # Option 1: Re-analyze files from Azure (if you want fresh analysis from Azure)
        print(f"Re-analyzing SQL files from Azure ADLS...")
        azure_results = analyzer.analyze_sql_scripts(container_name, input_path)
        
        if azure_results:
            results_to_upload = azure_results
            print(f"Using fresh analysis from Azure: {len(azure_results)} files")
        else:
            results_to_upload = results
            print(f"Using local analysis results: {len(results)} files")
        
        # Generate CSV content
        csv_content = analyzer.results_to_csv(results_to_upload)
        
        # Upload results to ADLS
        print(f"Uploading results to Azure ADLS: {output_path}")
        success = analyzer.upload_csv_to_adls(container_name, output_path, csv_content)
        
        if success:
            print("✅ Successfully uploaded results to Azure ADLS!")
            return True
        else:
            print("❌ Failed to upload results to Azure ADLS")
            return False
            
    except Exception as e:
        logger.error(f"Azure connection/upload failed: {str(e)}")
        print(f"❌ Azure operation failed: {str(e)}")
        return False

def main():
    """Main execution function with separated local analysis and Azure upload."""
    
    # Configuration
    LOCAL_MODE = True  # Set to True to analyze local files first
    local_sql_directory = "Profiler/sql_test_files_v2"  # Local directory containing SQL files
    local_output_csv = "Profiler/output/sql_complexity_report.csv"  # Local output path
    
    # Azure configuration (used only when uploading)
    account_name = "your_adls_account_name"  # Replace with actual account name
    container_name = "your_container_name"   # Replace with actual container name
    azure_input_path = "sql/scripts"         # Path to SQL files in Azure
    azure_output_path = "output/sql_complexity_report.csv"  # Output CSV path in Azure
    
    results = []
    
    try:
        if LOCAL_MODE:
            print("="*60)
            print("PHASE 1: LOCAL ANALYSIS")
            print("="*60)
            
            # Analyze local SQL files
            logger.info("Starting local SQL complexity analysis...")
            results = analyze_local_files(local_sql_directory)
            
            if not results:
                logger.warning("No SQL files found or analyzed successfully")
                return
            
            # Save results locally
            success, actual_path = save_local_csv(results, local_output_csv)
            if success:
                print(f"\n📁 Local CSV saved to: {actual_path}")
                local_output_csv = actual_path  # Update the path for later reference
            else:
                print(f"\n❌ Failed to save local CSV to: {local_output_csv}")
        
        # Display results summary
        if results:
            print("\n" + "="*60)
            print("ANALYSIS RESULTS SUMMARY")
            print("="*60)
            
            analyzer_instance = SQLComplexityAnalyzer.__new__(SQLComplexityAnalyzer)
            summary = analyzer_instance.generate_summary_report(results)
            print(summary)
            
            # Show detailed results for top 10 most complex files
            print("\nDetailed Results (Top 10 Most Complex Files):")
            print("-" * 100)
            print(f"{'Filename':<35} {'Class':<10} {'LOC':<6} {'Score':<8} {'Joins':<6} {'CTEs':<6} {'SubQ':<6} {'WinFn':<6}")
            print("-" * 100)
            
            for i, result in enumerate(results[:10]):
                filename = result['filename'][:33] + '..' if len(result['filename']) > 35 else result['filename']
                print(f"{filename:<35} {result['classification']:<10} {result['lines_of_code']:<6} "
                      f"{result['complexity_score']:<8} {result['joins']:<6} {result['ctes']:<6} "
                      f"{result['subqueries']:<6} {result['window_functions']:<6}")
        
        # Ask user if they want to proceed with Azure upload
        print("\n" + "="*60)
        print("PHASE 2: AZURE UPLOAD (OPTIONAL)")
        print("="*60)
        
        while True:
            user_choice = input("\nDo you want to connect to Azure and upload results? (y/n/quit): ").lower().strip()
            
            if user_choice in ['y', 'yes']:
                print(f"\nProceeding with Azure upload...")
                print(f"Account: {account_name}")
                print(f"Container: {container_name}")
                print(f"Output path: {azure_output_path}")
                
                success = connect_and_upload_to_azure(
                    results, account_name, container_name, 
                    azure_input_path, azure_output_path
                )
                
                if success:
                    print("\n🎉 Complete! Results available both locally and in Azure ADLS.")
                else:
                    print(f"\n⚠️  Results are still available locally at: {local_output_csv}")
                break
                
            elif user_choice in ['n', 'no']:
                print(f"\nSkipping Azure upload. Results available locally at: {local_output_csv}")
                break
                
            elif user_choice in ['quit', 'q', 'exit']:
                print("Exiting...")
                return
                
            else:
                print("Please enter 'y' for yes, 'n' for no, or 'quit' to exit.")
        
        logger.info("Analysis workflow completed!")
            
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        if results:
            print(f"Partial results may be available locally at: {local_output_csv}")
        raise

if __name__ == "__main__":
    main()