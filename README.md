# Bible Verse Analyzer

This program analyzes HTML articles to extract Bible verses, count their occurrences, and match them with biblical figures from tPerson.csv.

## Features

- Extracts Bible verses from HTML articles
- Counts verse occurrences
- Matches verses with biblical figures
- Generates CSV output with analysis results
- Comprehensive error logging

## Installation

1. Ensure you have Python 3.8 or higher installed
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Place your HTML articles in the `EN/EnArticle/` or `ZH/EnArticle/` directory, and chouse a Language type `EN` or `ZH`.
2. Ensure `tPerson.csv` is in the root directory
3. Run the program:
   ```bash
   python bible_verse_analyzer.py
   ```
4. Results will be saved in `verse_analysis_results.csv`
5. Check `bible_verse_analyzer.log` for any errors or warnings


## Configuration

You can modify the following settings in the `Config` class in `bible_verse_analyzer.py`:

- `ARTICLE_DIR`: Directory containing HTML articles
- `TPERSON_FILE`: Path to tPerson.csv
- `OUTPUT_FILE`: Path for output CSV
- `LANGUAGE`: Chouse Zh or En language type
- `VERSE_PATTERN_EN`: Regular expression for en bible verse matching
- `VERSE_PATTERN_ZH`: Regular expression for zh bible verse matching


## Output Format

The file 1 output CSV file contains the following columns:
- Verse: The Bible verse found
- Book: The Bible book found
- Count: Number of occurrences
- PersonID: ID of the biblical figure
- PersonName: Name of the biblical figure
- ZhName: Chinese name of the biblical figure
- File: The file path what the program analysis 

The file 2 output CSV file contains the following columns:
- PersonID: ID of the biblical figure
- PersonName: Name of the biblical figure
- File: The file path what the program analysis 
- Count: Number of occurrences


## Error Handling

The program logs all errors and warnings to `bible_verse_analyzer.log`. Common errors include:
- File not found
- Invalid verse format
- HTML parsing errors
- CSV parsing errors 