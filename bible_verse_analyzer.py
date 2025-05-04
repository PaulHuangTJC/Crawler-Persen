import os
import re
import csv
import logging
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path

# Configuration class
@dataclass
class Config:
    # 設定檔案路徑
    ARTICLE_DIR: str = "Crawler-Persen/ZH/article/"
    TPERSON_FILE: str = "Crawler-Persen/tPerson.csv"
    OUTPUT_FILE: str = "Crawler-Persen/verse_analysis_results_0502.csv"
    LANGUAGE: str = "zh"  # 'zh' for Chinese, 'en' for English
    # 正規表達式模式
    # 支援英文
    VERSE_PATTERN_EN: str = r"([1-3]?\s*[A-Za-z]+)\s*(\d+):(\d+)"
    # 支援中文全名、中文縮寫
    VERSE_PATTERN_ZH: str = (
        r"《?("
        r"創世記|出埃及記|利未記|民數記|申命記|約書亞記|士師記|路得記|"
        r"撒母耳記上|撒母耳記下|列王紀上|列王紀下|歷代志上|歷代志下|"
        r"以斯拉記|尼希米記|以斯帖記|約伯記|詩篇|箴言|傳道書|雅歌|"
        r"以賽亞書|耶利米書|耶利米哀歌|以西結書|但以理書|何西阿書|"
        r"約珥書|阿摩司書|俄巴底亞書|約拿書|彌迦書|那鴻書|哈巴谷書|"
        r"西番雅書|哈該書|撒迦利亞書|瑪拉基書|馬太福音|馬可福音|"
        r"路加福音|約翰福音|使徒行傳|羅馬書|哥林多前書|哥林多後書|"
        r"加拉太書|以弗所書|腓立比書|歌羅西書|帖撒羅尼迦前書|"
        r"帖撒羅尼迦後書|提摩太前書|提摩太後書|提多書|腓利門書|"
        r"希伯來書|雅各書|彼得前書|彼得後書|約翰一書|約翰二書|"
        r"約翰三書|猶大書|啟示錄|"
        r"創|出|利|民|申|書|士|得|撒上|撒下|王上|王下|代上|代下|拉|尼|斯|伯|詩|箴|傳|雅|賽|耶|哀|結|但|何|珥|摩|俄|拿|彌|鴻|哈|番|該|亞|瑪|太|可|路|約|徒|羅|林前|林後|加|弗|腓|西|帖前|帖後|提前|提後|多|門|來|雅各|彼前|彼後|約一|約二|約三|猶|啟"
        r")》?(\d+|[一二三四五六七八九十百廿]*)章?(\d+)節?"
    )
    # ([一二三四五六七八九十百廿]*)(\d+)
    ERROR_MESSAGES = {
        "file_not_found": "Error: File not found: {}",
        "invalid_verse": "Warning: Invalid verse format: {}",
        "parse_error": "Error parsing file: {}"
    }

class VerseExtractor:
    def __init__(self, article_path: str):
        self.article_path = article_path
        # Compile regex patterns for English and Chinese
        if Config.LANGUAGE == 'zh':
            self.verse_pattern = re.compile(Config.VERSE_PATTERN_ZH)
        else:
            self.verse_pattern = re.compile(Config.VERSE_PATTERN_EN)
        
    def normalize_verse(self, book: str, chapter: str, verse: str) -> str:
        """
        Normalize verse format to match tPerson.csv format
        """
        # Convert book name to standard format (e.g., "Exodus" -> "Exod")
        book = book.strip().lower()
        chapter = chapter.strip().lower()
        book_mapping = {
            # 英文
            'exodus': 'Exod',
            'genesis': 'Gen',
            'leviticus': 'Lev',
            'numbers': 'Num',
            'deuteronomy': 'Deut',
            'joshua': 'Josh',
            'judges': 'Judg',
            'ruth': 'Ruth',
            '1 samuel': '1Sam',
            '2 samuel': '2Sam',
            '1 kings': '1Kgs',
            '2 kings': '2Kgs',
            '1 chronicles': '1Chr',
            '2 chronicles': '2Chr',
            'ezra': 'Ezra',
            'nehemiah': 'Neh',
            'esther': 'Esth',
            'job': 'Job',
            'psalms': 'Ps',
            'proverbs': 'Prov',
            'ecclesiastes': 'Eccl',
            'song of solomon': 'Song',
            'isaiah': 'Isa',
            'jeremiah': 'Jer',
            'lamentations': 'Lam',
            'ezekiel': 'Ezek',
            'daniel': 'Dan',
            'hosea': 'Hos',
            'joel': 'Joel',
            'amos': 'Amos',
            'obadiah': 'Obad',
            'jonah': 'Jonah',
            'micah': 'Mic',
            'nahum': 'Nah',
            'habakkuk': 'Hab',
            'zephaniah': 'Zeph',
            'haggai': 'Hag',
            'zechariah': 'Zech',
            'malachi': 'Mal',
            'matthew': 'Matt',
            'mark': 'Mark',
            'luke': 'Luke',
            'john': 'John',
            'acts': 'Acts',
            'romans': 'Rom',
            '1 corinthians': '1Cor',
            '2 corinthians': '2Cor',
            'galatians': 'Gal',
            'ephesians': 'Eph',
            'philippians': 'Phil',
            'colossians': 'Col',
            '1 thessalonians': '1Thess',
            '2 thessalonians': '2Thess',
            '1 timothy': '1Tim',
            '2 timothy': '2Tim',
            'titus': 'Titus',
            'philemon': 'Phlm',
            'hebrews': 'Heb',
            'james': 'James',
            '1 peter': '1Pet',
            '2 peter': '2Pet',
            '1 john': '1John',
            '2 john': '2John',
            '3 john': '3John',
            'jude': 'Jude',
            'revelation': 'Rev',
            # 繁體中文
            '創世記': 'Gen',
            '出埃及記': 'Exod',
            '利未記': 'Lev',
            '民數記': 'Num',
            '申命記': 'Deut',
            '約書亞記': 'Josh',
            '士師記': 'Judg',
            '路得記': 'Ruth',
            '撒母耳記上': '1Sam',
            '撒母耳記下': '2Sam',
            '列王紀上': '1Kgs',
            '列王紀下': '2Kgs',
            '歷代志上': '1Chr',
            '歷代志下': '2Chr',
            '以斯拉記': 'Ezra',
            '尼希米記': 'Neh',
            '以斯帖記': 'Esth',
            '約伯記': 'Job',
            '詩篇': 'Ps',
            '箴言': 'Prov',
            '傳道書': 'Eccl',
            '雅歌': 'Song',
            '以賽亞書': 'Isa',
            '耶利米書': 'Jer',
            '耶利米哀歌': 'Lam',
            '以西結書': 'Ezek',
            '但以理書': 'Dan',
            '何西阿書': 'Hos',
            '約珥書': 'Joel',
            '阿摩司書': 'Amos',
            '俄巴底亞書': 'Obad',
            '約拿書': 'Jonah',
            '彌迦書': 'Mic',
            '那鴻書': 'Nah',
            '哈巴谷書': 'Hab',
            '西番雅書': 'Zeph',
            '哈該書': 'Hag',
            '撒迦利亞書': 'Zech',
            '瑪拉基書': 'Mal',
            '馬太福音': 'Matt',
            '馬可福音': 'Mark',
            '路加福音': 'Luke',
            '約翰福音': 'John',
            '使徒行傳': 'Acts',
            '羅馬書': 'Rom',
            '哥林多前書': '1Cor',
            '哥林多後書': '2Cor',
            '加拉太書': 'Gal',
            '以弗所書': 'Eph',
            '腓立比書': 'Phil',
            '歌羅西書': 'Col',
            '帖撒羅尼迦前書': '1Thess',
            '帖撒羅尼迦後書': '2Thess',
            '提摩太前書': '1Tim',
            '提摩太後書': '2Tim',
            '提多書': 'Titus',
            '腓利門書': 'Phlm',
            '希伯來書': 'Heb',
            '雅各書': 'James',
            '彼得前書': '1Pet',
            '彼得後書': '2Pet',
            '約翰一書': '1John',
            '約翰二書': '2John',
            '約翰三書': '3John',
            '猶大書': 'Jude',
            '啟示錄': 'Rev',
            # 繁體中文縮寫
            '創': 'Gen',
            '出': 'Exod',
            '利': 'Lev',
            '民': 'Num',
            '申': 'Deut',
            '書': 'Josh',
            '士': 'Judg',
            '得': 'Ruth',
            '撒上': '1Sam',
            '撒下': '2Sam',
            '王上': '1Kgs',
            '王下': '2Kgs',
            '代上': '1Chr',
            '代下': '2Chr',
            '拉': 'Ezra',
            '尼': 'Neh',
            '斯': 'Esth',
            '伯': 'Job',
            '詩': 'Ps',
            '箴': 'Prov',
            '傳': 'Eccl',
            '雅': 'Song',
            '賽': 'Isa',
            '耶': 'Jer',
            '哀': 'Lam',
            '結': 'Ezek',
            '但': 'Dan',
            '何': 'Hos',
            '珥': 'Joel',
            '摩': 'Amos',
            '俄': 'Obad',
            '拿': 'Jonah',
            '彌': 'Mic',
            '鴻': 'Nah',
            '哈': 'Hab',
            '番': 'Zeph',
            '該': 'Hag',
            '亞': 'Zech',
            '瑪': 'Mal',
            '太': 'Matt',
            '可': 'Mark',
            '路': 'Luke',
            '約': 'John',
            '徒': 'Acts',
            '羅': 'Rom',
            '林前': '1Cor',
            '林後': '2Cor',
            '加': 'Gal',
            '弗': 'Eph',
            '腓': 'Phil',
            '西': 'Col',
            '帖前': '1Thess',
            '帖後': '2Thess',
            '提前': '1Tim',
            '提後': '2Tim',
            '多': 'Titus',
            '門': 'Phlm',
            '來': 'Heb',
            '雅各': 'James',
            '彼前': '1Pet',
            '彼後': '2Pet',
            '約一': '1John',
            '約二': '2John',
            '約三': '3John',
            '猶': 'Jude',
            '啟': 'Rev'
            
        }
        
        booksw = str
        # Try to find a match in the mapping
        for key, value in book_mapping.items():
            if book.startswith(key):
                # return f"{value} {chapter}:{verse}"
                booksw = value

        # 中文數字轉換
        chinese_numbers = {
            '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
            '六': '6', '七': '7', '八': '8', '九': '9', '十': '10',
            '十一': '11', '十二': '12', '十三': '13', '十四': '14', '十五': '15',
            '十六': '16', '十七': '17', '十八': '18', '十九': '19', 
            '廿一' : '21','廿二' : '22','廿三' : '23','廿四' : '24','廿五' : '25',
            '廿六' : '26','廿七' : '27','廿八' : '28','廿九' : '29','廿' : '20',
            '卅ㄧ' : '31','卅二' : '32','卅三' : '33','卅四' : '34','卅五' : '35',
            '卅六' : '36','卅七' : '37','卅八' : '38','卅九' : '39','卅' : '30', 
            '二十一': '21', '二十二': '22', '二十三': '23', '二十四': '24', '二十五': '25',
            '二十六': '26', '二十七': '27', '二十八': '28', '二十九': '29', '三十': '30',
            '三十一': '31', '三十二': '32', '三十三': '33', '三十四': '34', '三十五': '35',
            '三十六': '36', '三十七': '37', '三十八': '38', '三十九': '39', '四十': '40',
            '四十一': '41', '四十二': '42', '四十三': '43', '四十四': '44', '四十五': '45',
            '四十六': '46', '四十七': '47', '四十八': '48', '四十九': '49', '五十': '50',
            '五十一': '51', '五十二': '52', '五十三': '53', '五十四': '54', '五十五': '55',
            '五十六': '56', '五十七': '57', '五十八': '58', '五十九': '59', '六十': '60',
            '六十一': '61', '六十二': '62', '六十三': '63', '六十四': '64', '六十五': '65',
            '六十六': '66', '六十七': '67', '六十八': '68', '六十九': '69', '七十': '70',
            '七十一': '71', '七十二': '72', '七十三': '73', '七十四': '74', '七十五': '75',
            '七十六': '76', '七十七': '77', '七十八': '78', '七十九': '79', '八十': '80',
            '八十一': '81', '八十二': '82', '八十三': '83', '八十四': '84', '八十五': '85',
            '八十六': '86', '八十七': '87', '八十八': '88', '八十九': '89', '九十': '90',
            '九十一': '91', '九十二': '92', '九十三': '93', '九十四': '94', '九十五': '95',
            '九十六': '96', '九十七': '97', '九十八': '98', '九十九': '99', '一百': '100',
            '一百零一': '101', '一百零二': '102', '一百零三': '103', '一百零四': '104', '一百零五': '105',
            '一百零六': '106', '一百零七': '107', '一百零八': '108', '一百零九': '109', 
            '二一': '21', '二二': '22', '二十三': '23', '二十四': '24', '二十五': '25',
            '二六': '26', '二七': '27', '二八': '28', '二九': '29', '三十': '30',
            '三一': '31', '三二': '32', '三三': '33', '三四': '34', '三五': '35',
            '三六': '36', '三七': '37', '三八': '38', '三九': '39', '四十': '40',
            '四一': '41', '四二': '42', '四三': '43', '四四': '44', '四五': '45',
            '四六': '46', '四七': '47', '四八': '48', '四九': '49', '五十': '50',
            '五一': '51', '五二': '52', '五三': '53', '五四': '54', '五五': '55',
            '五六': '56', '五七': '57', '五八': '58', '五九': '59', '六十': '60',
            '六一': '61', '六二': '62', '六三': '63', '六四': '64', '六五': '65',
            '六六': '66', '六七': '67', '六八': '68', '六九': '69', '七十': '70',
            '七一': '71', '七二': '72', '七三': '73', '七四': '74', '七五': '75',
            '七六': '76', '七七': '77', '七八': '78', '七九': '79', '八十': '80',
            '八一': '81', '八二': '82', '八三': '83', '八四': '84', '八五': '85',
            '八六': '86', '八七': '87', '八八': '88', '八九': '89', '九十': '90',
            '九一': '91', '九二': '92', '九三': '93', '九四': '94', '九五': '95',
            '九六': '96', '九七': '97', '九八': '98', '九九': '99','一○○': '100',
            '一○一': '101', '一○二': '102', '一○三': '103', '一○四': '104', '一○五': '105',
            '一○六': '106', '一○七': '107', '一○八': '108', '一○九': '109','一百一十': '110',
            '一一一': '111', '一一二': '112', '一一三': '113', '一一四': '114', '一一五': '115',
            '一一六': '116', '一一七': '117', '一一八': '118', '一一九': '119', '一二○': '120',
            '一二一': '121', '一二二': '122', '一二三': '123', '一二四': '124', '一二五': '125',
            '一二六': '126', '一二七': '127', '一二八': '128', '一二九': '129', '一三○': '130',
            '一三一': '131', '一三二': '132', '一三三': '133', '一三四': '134', '一三五': '135',
            '一三六': '136', '一三七': '137', '一三八': '138', '一三九': '139', '一四○': '140',
            '一四一': '141', '一四二': '142', '一四三': '143', '一四四': '144', '一四五': '145',
            '一四六': '146', '一四七': '147', '一四八': '148', '一四九': '149', '一五○': '150',
            '一百一十': '110','廿' : '20',
            '一百一十一': '111', '一百一十二': '112', '一百一十三': '113', '一百一十四': '114', '一百一十五': '115',
            '一百一十六': '116', '一百一十七': '117', '一百一十八': '118', '一百一十九': '119', '一百二十': '120',
            '一百二十一': '121', '一百二十二': '122', '一百二十三': '123', '一百二十四': '124', '一百二十五': '125',
            '一百二十六': '126', '一百二十七': '127', '一百二十八': '128', '一百二十九': '129', '一百三十': '130',
            '一百三十一': '131', '一百三十二': '132', '一百三十三': '133', '一百三十四': '134', '一百三十五': '135',
            '一百三十六': '136', '一百三十七': '137', '一百三十八': '138', '一百三十九': '139', '一百四十': '140',
            '一百四十一': '141', '一百四十二': '142', '一百四十三': '143', '一百四十四': '144', '一百四十五': '145',
            '一百四十六': '146', '一百四十七': '147', '一百四十八': '148', '一百四十九': '149', '一百五十': '150'
        }
        
        chaptersw = chapter if chapter != '' else 1
        # Sort chinese_numbers items by key length in descending order to match longer patterns first
        sorted_chinese_numbers = sorted(chinese_numbers.items(), key=lambda x: len(x[0]), reverse=True)
        for key, value in sorted_chinese_numbers:
            if chapter.startswith(key):
                chaptersw = value
                break
        # ... existing code ...
        if Config.LANGUAGE == 'zh':
            return f"{booksw} {chaptersw}:{verse}"
        else:
            return f"{booksw} {chapter}:{verse}"
        # If no match found, return the original format
        # return f"{book} {chapter}:{verse}"
        
    def extract_verses(self) -> List[str]:
        """
        Extract Bible verses from HTML articles
        Returns: List of verses in format "Book Chapter:Verse"
        """
        try:
            # print(f"Processing file: {self.article_path}")
            with open(self.article_path, 'r', encoding='utf-8') as file:
                content = file.read()
                # Use BeautifulSoup for HTML/HTM, plain text for .txt
                if Path(self.article_path).suffix.lower() in ['.html', '.htm']:
                    soup = BeautifulSoup(content, 'html.parser')
                    text = soup.get_text()
                # else:
                # text = content
                # print(text[:1000])  # Print first 1000 characters for debugging
                # Extract verses using regex
                verses = self.verse_pattern.findall(text)
                print(f"Extracted {len(verses)}, sample: {verses[:]}")
                # Normalize verses
                formatted_verses = [self.normalize_verse(book, chapter, verse) for book, chapter, verse in verses]
                print(f"Found {len(formatted_verses)} verses in {self.article_path}")
                if formatted_verses:
                    print(f"Sample verses found: {formatted_verses[:]}")
                return formatted_verses
        except Exception as e:
            logging.error(f"Error extracting verses from {self.article_path}: {str(e)}")
            print(f"Error processing {self.article_path}: {str(e)}")
            return []

class VerseCounter:
    # def __init__(self):
        # self.verse_counts: Dict[str, int] = {}
        
    def count_verses(self, verses: List[str]) -> Dict[str, int]:
        """
        Count occurrences of each verse
        Returns: Dictionary with verses as keys and counts as values
        """
        self.verse_counts: Dict[str, int] = {}

        for verse in verses:
            self.verse_counts[verse] = self.verse_counts.get(verse, 0) + 1
        # print(f"Total unique verses found: {len(self.verse_counts)}")
        # if self.verse_counts:
        #     print(f"Sample verse counts: {list(self.verse_counts.items())[:3]}")
        return self.verse_counts


class PersonVerseMatcher:
    def __init__(self, tperson_file: str):
        self.person_verses: Dict[str, List[Dict]] = {}
        self.load_person_verses(tperson_file)
        
    def load_person_verses(self, file_path: str) -> None:
        """
        Load and parse tPerson.csv
        """
        try:
            # print(f"Loading person verses from {file_path}")
            df = pd.read_csv(file_path)
            # print(f"Loaded {len(df)} rows from tPerson.csv")
            
            for _, row in df.iterrows():
                verses = str(row['Verses']).split(';')
                for verse in verses:
                    verse = verse.strip()
                    if verse:
                        if verse not in self.person_verses:
                            self.person_verses[verse] = []
                        self.person_verses[verse].append({
                            'PersonID': row['PersonID'],
                            'Name': row['Name'],
                            'ZhName': row['ZhName']
                        })
            print(f"Loaded {len(self.person_verses)} unique verses from tPerson.csv")
            if self.person_verses:
                print(f"Sample person verses: {list(self.person_verses.items())[:3]}")
        except Exception as e:
            logging.error(f"Error loading person verses from {file_path}: {str(e)}")
            print(f"Error loading person verses: {str(e)}")
            
    def find_matching_persons(self, verse: str) -> List[Dict]:
        """
        Find biblical figures associated with given verse
        Returns: List of matching person dictionaries
        """
        return self.person_verses.get(verse, [])

class ErrorLogger:
    def __init__(self, log_file: str = "bible_verse_analyzer.log"):
        self.log_file = log_file
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def log_error(self, error_type: str, message: str) -> None:
        """
        Log errors with timestamp and type
        """
        logging.error(f"{error_type}: {message}")

def main():
    # Initialize components
    config = Config()
    error_logger = ErrorLogger()
    
    # Load person-verse data
    person_matcher = PersonVerseMatcher(config.TPERSON_FILE)
    verse_counter = VerseCounter()
    
    # Process HTML articles
    results = []
    article_dir = Path(config.ARTICLE_DIR)
    
    if not article_dir.exists():
        error_logger.log_error("file_not_found", f"Article directory not found: {config.ARTICLE_DIR}")
        print(f"Error: Article directory not found: {config.ARTICLE_DIR}")
        return
    
    # Search for all .html, .htm, .txt files recursively
    file_patterns = ["*.htm", "*.html"]
    
    for pattern in file_patterns:
        html_files = []
        print(f"Looking for {pattern} files in {article_dir}")
        html_files.extend(article_dir.rglob(pattern))
        print(f"Found {len(html_files)} {pattern} files to process")
        
        for html_file in html_files:
            # Initialize variables for each file
            extractor = None
            verses = []
            verse_counts = {}
            
            try:
                extractor = VerseExtractor(str(html_file))
                verses = extractor.extract_verses()
                verse_counts = verse_counter.count_verses(verses)
                
                # Find matching persons for each verse
                for verse, count in verse_counts.items():
                    matching_persons = person_matcher.find_matching_persons(verse)
                    # if matching_persons:
                        # print(f"Found {len(matching_persons)} matching persons for verse: {verse}")
                    # 取得 book 名稱（以空格分割，取第一個）
                    book = verse.split(' ', 1)[0] if ' ' in verse else verse
                    for person in matching_persons:
                        results.append({
                            'Verse': verse,
                            'Book': book,  # 新增欄位
                            'Count': count,
                            'PersonID': person['PersonID'],
                            'PersonName': person['Name'],
                            'ZhName': person['ZhName'],
                            'File': str(html_file)
                        })
                
                print(f"Finished processing file: {html_file}, verse_counts: {list(verse_counts.items())[:3]}")
                # Clear variables after processing each file
                extractor = None
                verses = []
                verse_counts = {}
                
                
            except Exception as e:
                error_logger.log_error("processing_error", f"Error processing file {html_file}: {str(e)}")
                print(f"Error processing file {html_file}: {str(e)}")
                continue
    
    print(f"Total results found: {len(results)}")
    
    # Write results to CSV
    try:
        if results:
            df = pd.DataFrame(results)
            df.to_csv(config.OUTPUT_FILE, index=False)
            print(f"Results written to {config.OUTPUT_FILE}")
            print(f"Number of rows written: {len(df)}")
            
            # 彙總
            summary = df.groupby(['PersonID', 'PersonName', 'File'], as_index=False)['Count'].sum()
            summary_file = config.OUTPUT_FILE.replace('.csv', '_summary.csv')
            summary.to_csv(summary_file, index=False)
            print(f"Summary written to {summary_file}")
            print(f"Number of summary rows: {len(summary)}")
        else:
            print("No results found to write to CSV")
    except Exception as e:
        error_logger.log_error("output_error", f"Error writing results: {str(e)}")
        print(f"Error writing results: {str(e)}")

if __name__ == "__main__":
    main()