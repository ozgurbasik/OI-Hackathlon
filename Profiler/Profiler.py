import re
import csv
import io
import os
import logging
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLComplexityAnalyzer:
    """Analyzes SQL scripts for complexity metrics and generates reports."""
    
    def __init__(self):
        """Initialize the analyzer."""
        logger.info("SQL Complexity Analyzer initialized")
        
    def read_sql_files(self, directory_path: str) -> Dict[str, str]:
        """Read all SQL files from specified directory."""
        sql_files = {}
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
                                logger.info(f"Read SQL file: {relative_path}")
                        except Exception as e:
                            logger.warning(f"Failed to read file {file_path}: {str(e)}")
                            
        except Exception as e:
            logger.error(f"Error reading directory: {str(e)}")
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
        
        # Enhanced complexity patterns with adjusted weights
        patterns = {
            'join': (r'\b(?:inner\s+join|left\s+join|right\s+join|full\s+join|cross\s+join|join)\b', 2),
            'cte': (r'\bwith\b\s+[a-zA-Z_][a-zA-Z0-9_]*\s+as\s*\(', 4),  # Increased weight for CTEs
            'subquery': (r'\(\s*select\b', 2),
            'window_function': (r'\bover\s*\([^)]*\)', 4),  # Increased weight for window functions
            'union': (r'\bunion\s+(?:all\s+)?select\b', 2),
            'case_when': (r'\bcase\b.*?\bwhen\b', 1),
            'aggregate': (r'\b(?:sum|count|avg|min|max|group_concat)\s*\(', 1),
            'having': (r'\bhaving\b', 2),
            'exists': (r'\b(?:exists|not\s+exists)\b', 2),
            'recursive': (r'\bwith\s+recursive\b', 5),  # Increased weight
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
        Classify script complexity with adjusted thresholds:
        - Simple: score <= 40
        - Medium: score 41-67 
        - Complex: score >= 68
        
        Also considers LOC as a secondary factor.
        """
        
        # Primary classification based on complexity score
        if score <= 40:
            primary_classification = 'Simple'
            base_reason = f'Low complexity score ({score})'
        elif score <= 67:
            primary_classification = 'Medium'
            base_reason = f'Medium complexity score ({score})'
        else:
            primary_classification = 'Complex'
            base_reason = f'High complexity score ({score})'
        
        # Secondary consideration: LOC can bump up classification
        complexity_indicators = []
        
        # Very large files should be at least Medium, regardless of score
        if loc > 250 and primary_classification == 'Simple':
            primary_classification = 'Medium'
            complexity_indicators.append('large file size despite simple patterns')
        elif loc > 400 and primary_classification == 'Medium':
            primary_classification = 'Complex'
            complexity_indicators.append('very large file size')
        
        # Add LOC context to reason
        if loc <= 50:
            loc_context = 'short script'
        elif loc <= 150:
            loc_context = 'moderate length script'
        elif loc <= 300:
            loc_context = 'large script'
        else:
            loc_context = 'very large script'
        
        # Build detailed reason
        reason_parts = [base_reason, loc_context]
        
        if complexity_indicators:
            reason_parts.extend(complexity_indicators)
        
        reason = ', '.join(reason_parts)
        score_detail = f"LOC={loc}, Complexity={score}"
        
        return primary_classification, score_detail, reason
    
    def analyze_sql_scripts(self, directory_path: str) -> List[Dict]:
        """Analyze all SQL scripts and return complexity metrics."""
        try:
            sql_files = self.read_sql_files(directory_path)
            
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

Classification Distribution (Based on complexity score thresholds):
  Simple (score <= 40): Basic queries with minimal complexity
  Medium (score 41-67): Moderate complexity with joins, CTEs, some advanced features  
  Complex (score >= 68): High complexity with many advanced SQL features
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

def get_next_available_filename(base_path: str) -> str:
    """Get next available filename if the base path already exists."""
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

def save_csv(results: List[Dict], output_path: str) -> Tuple[bool, str]:
    """Save results to local CSV file. Returns success status and actual path used."""
    try:
        if not results:
            logger.warning("No results to save")
            return False, output_path
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Get next available filename if original exists
        actual_output_path = get_next_available_filename(output_path)
        
        # Generate CSV content
        analyzer = SQLComplexityAnalyzer()
        csv_content = analyzer.results_to_csv(results)
        
        # Write to file
        with open(actual_output_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        logger.info(f"Successfully saved CSV to: {actual_output_path}")
        return True, actual_output_path
        
    except Exception as e:
        logger.error(f"Failed to save CSV file: {str(e)}")
        return False, output_path

def main():
    """Main execution function."""
    
    # Configuration
    sql_directory = "Profiler/sql_test_files_v2"  # Directory containing SQL files
    output_csv = "Profiler/output/sql_complexity_report.csv"  # Output path
    
    try:
        print("="*60)
        print("SQL COMPLEXITY ANALYSIS")
        print("="*60)
        
        # Initialize analyzer
        analyzer = SQLComplexityAnalyzer()
        
        # Analyze SQL files
        logger.info("Starting SQL complexity analysis...")
        results = analyzer.analyze_sql_scripts(sql_directory)
        
        if not results:
            logger.warning("No SQL files found or analyzed successfully")
            return
        
        # Save results to CSV
        success, actual_path = save_csv(results, output_csv)
        if success:
            print(f"\n📁 CSV saved to: {actual_path}")
        else:
            print(f"\n❌ Failed to save CSV to: {output_csv}")
        
        # Display results summary
        print("\n" + "="*60)
        print("ANALYSIS RESULTS SUMMARY")
        print("="*60)
        
        summary = analyzer.generate_summary_report(results)
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
        
        logger.info("Analysis completed successfully!")
        print(f"\n Analysis complete! Results saved to: {actual_path}")
            
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()