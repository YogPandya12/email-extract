from flask import Flask, request, render_template, send_file
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import os
import logging
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse, urljoin
import time
import random
from requests_html import HTMLSession
import asyncio
import nest_asyncio
from pyppeteer import launch
# Apply nest_asyncio to allow nested event loops (needed for requests_html in threads)
nest_asyncio.apply()

app = Flask(__name__)
application = app

def find_url_column(columns):
    keywords = ['website', 'url', 'websites', 'urls']
    for col in columns:
        if any(keyword in col.lower() for keyword in keywords):
            return col
    return None

def extract_emails_from_url(url):
    if pd.isna(url) or not isinstance(url, str):
        return ""
    if not url.startswith(('http://', 'https://')):
        url = f"http://{url}"
    visited_urls = set()
    emails = set()
    keywords = ['contact', 'about', 'get in touch', 'reach us', 'communication','contacts','about the company','contact us']
    
    base_domain = urlparse(url).netloc

    def is_internal_link(link):
        return urlparse(link).netloc == base_domain or urlparse(link).netloc == ""
    
    def fetch_emails(current_url):
        if current_url in visited_urls:
            return
        visited_urls.add(current_url)
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
            }
            response = requests.get(current_url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            text = ' '.join(soup.stripped_strings)
            raw_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            emails.update([email for email in raw_emails if not email[0].isdigit()])
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith("mailto:"):
                    email = href.replace("mailto:", "").split("?")[0]
                    if email and not email[0].isdigit():
                        emails.add(email)
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(current_url, href)
                if is_internal_link(full_url) and any(keyword in href.lower() for keyword in keywords):
                    fetch_emails(full_url)
        except requests.exceptions.RequestException as e:
            print(f"RequestException for {current_url}: {e}")
        except Exception as e:
            print(f"Error processing {current_url}: {e}")

    fetch_emails(url)
    return ', '.join(emails) if emails else ""

# Functions from the second code for JS rendering backup

def extract_emails(soup):
    """
    Extract emails from a BeautifulSoup object by checking mailto links, text content, and obfuscated emails.
    Returns a set of unique, lowercased emails.
    """
    emails = set()
    
    for a_tag in soup.find_all('a', href=True):
        if a_tag['href'].startswith('mailto:'):
            email = a_tag['href'].split('?')[0].replace('mailto:', '').strip().lower()
            if validate_email(email):
                emails.add(email)
    
    text = ' '.join(soup.stripped_strings)
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}'
    potential_emails = re.findall(email_pattern, text)
    for email in potential_emails:
        if validate_email(email): 
            emails.add(email.lower())
    
    script_tags = soup.find_all('script')
    for script in script_tags:
        script_text = script.get_text()
        emails.update(extract_obfuscated_emails(script_text))
    
    return emails

def extract_obfuscated_emails(text):
    """
    Extract emails obfuscated in JavaScript or HTML (e.g., string concatenation or character entities).
    Returns a set of validated emails.
    """
    emails = set()
    
    concat_pattern = r'[\'"][a-zA-Z0-9._%+-]+[\'"]\s*\+\s*[\'"]\@[\'"]\s*\+\s*[\'"][a-zA-Z0-9.-]+\.[A-Za-z]{2,}[\'"]'
    matches = re.findall(concat_pattern, text)
    for match in matches:
        parts = re.findall(r'[\'"]([^\'"]*)[\'"]\s*\+\s*', match + "+")
        reconstructed = ''.join(parts)
        if '@' in reconstructed and validate_email(reconstructed):
            emails.add(reconstructed)
    
    text_decoded = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    emails.update(extract_emails_from_text(text_decoded))
    
    return emails

def extract_emails_from_text(text):
    """
    Extract emails from plain text using regex.
    Returns a set of validated emails.
    """
    if not text:
        return set()
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}'
    emails = set()
    matches = re.findall(email_pattern, text)
    for match in matches:
        email = match.lower().strip()
        if validate_email(email):
            emails.add(email)
    return emails

def validate_email(email):
    """
    Validate if a string is a proper email address, excluding common false positives.
    Returns True if valid, False otherwise.
    """
    if not email or '@' not in email:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}$'
    common_false_positives = [
        'example.com', 'domain.com', 'email.com', 'your-email.com',
        'username@', '@domain', 'example@example'
    ]
    
    if any(fp in email.lower() for fp in common_false_positives):
        return False
    
    local_part = email.split('@')[0]
    phone_pattern = r'\d{3}-\d{3}-\d{4}'
    if re.search(phone_pattern, local_part):
        return False
    
    return bool(re.match(pattern, email))

def find_subpage_urls(soup, base_url):
    """
    Find subpage URLs that might contain contact info based on keywords in link text and URL path.
    Returns a set of absolute URLs within the same domain.
    """
    keywords = {
        "en": ["contact", "about", "reach", "support", "help", "info", "team", "staff", "brokers", "get in touch", "our people", "meet the team", "directory", "contact us", "about us", "reach us"],
        "fr": ["contact", "à propos", "nous contacter", "support", "aide", "info", "équipe", "personnel", "courtiers", "nous joindre", "notre équipe", "rencontrer l'équipe", "annuaire"],
        "de": ["kontakt", "über uns", "erreichen", "unterstützung", "hilfe", "info", "team", "personal", "makler", "uns kontaktieren", "unser team", "team treffen", "verzeichnis"],
        "it": ["contatto", "chi siamo", "raggiungere", "supporto", "aiuto", "info", "team", "personale", "broker", "contattarci", "il nostro team", "incontrare il team", "directory"],
        "ur": ["رابط", "ہم سے رابطہ", "ہم تک پہنچیں", "حمایت", "مدد", "اطلاعات", "ٹیم", "افراد", "بروکر", "ہم سے رابطہ کریں", "ہماری ٹیم", "ٹیم سے ملاقات", "ڈائریکٹری"],
        "ar": ["اتصال", "معلومات عنا", "الوصول إلينا", "دعم", "مساعدة", "معلومات", "فريق", "أفراد", "سماسرة", "اتصل بنا", "فريقنا", "لقاء الفريق", "دليل"],
        "es": ["contacto", "acerca de", "alcanzarnos", "soporte", "ayuda", "info", "equipo", "personal", "corredores", "contactarnos", "nuestro equipo", "conocer al equipo", "directorio"],
        "pt": ["contato", "sobre", "alcançar", "suporte", "ajuda", "info", "equipe", "pessoal", "corretor", "contatar-nos", "nossa equipe", "conhecer a equipe", "diretório"],
        "ru": ["контакт", "о нас", "достичь", "поддержка", "помощь", "инфо", "команда", "персонал", "брокеры", "связаться с нами", "наша команда", "встреча команды", "справочник"],
        "zh": ["联系", "关于我们", "联系我们", "支持", "帮助", "信息", "团队", "人员", "经纪人", "联系我们", "我们的团队", "团队见面", "目录"],
        "ja": ["コンタクト", "私たちについて", "私たちに連絡する", "サポート", "ヘルプ", "情報", "チーム", "スタッフ", "ブローカー", "私たちに連絡する", "私たちのチーム", "チームに会う", "ディレクトリ"],
        "ko": ["연락처", "关于我们", "연락처", "지원", "도움말", "정보", "팀", "직원", "중개인", "연락처", "우리 팀", "팀 만남", "디렉토리"],
    }
    
    subpage_urls = set()
    base_netloc = urlparse(base_url).netloc
    
    # Detect language of the webpage (simplified approach)
    # For accurate detection, consider using a library like langdetect
    detected_language = "en"  # Default to English
    text = ' '.join(soup.stripped_strings)
    if "fr" in text.lower():
        detected_language = "fr"
    elif "de" in text.lower():
        detected_language = "de"
    elif "it" in text.lower():
        detected_language = "it"
    elif "ur" in text.lower():
        detected_language = "ur"
    elif "ar" in text.lower():
        detected_language = "ar"
    elif "es" in text.lower():
        detected_language = "es"
    elif "pt" in text.lower():
        detected_language = "pt"
    elif "ru" in text.lower():
        detected_language = "ru"
    elif "zh" in text.lower():
        detected_language = "zh"
    elif "ja" in text.lower():
        detected_language = "ja"
    elif "ko" in text.lower():
        detected_language = "ko"
    
    for a_tag in soup.find_all('a', href=True):
        link_text = a_tag.get_text().strip().lower()
        href = a_tag['href']
        abs_url = urljoin(base_url, href)
        parsed_url = urlparse(abs_url)
        
        # Use detected language's keywords
        if any(keyword in link_text for keyword in keywords.get(detected_language, keywords["en"])) or \
           any(keyword in parsed_url.path.lower() for keyword in keywords.get(detected_language, keywords["en"])):
            if parsed_url.netloc == base_netloc and parsed_url.scheme in ['http', 'https']:
                subpage_urls.add(abs_url)
    
    return subpage_urls

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

def js_render_with_session(url, headers, timeout=30):
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument(f"user-agent={headers['User-Agent']}")
        chrome_options.add_argument("--no-sandbox")  # Required for Docker
        chrome_options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
        
        # Automatically download and use the correct ChromeDriver version
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        time.sleep(3)  # Wait for JS to load
        html_content = driver.page_source
        driver.quit()
        return html_content
    except Exception as e:
        print(f"Error in JS rendering with Selenium: {str(e)}")
        return None

def find_emails_js(base_url, max_subpages=3, max_retries=2):
    """
    Extract emails from a website and its subpages using JavaScript rendering.
    Returns a string of comma-separated emails or "No email ID found".
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    if not base_url.startswith(('http://', 'https://')):
        base_url = 'https://' + base_url
    
    visited_urls = set()
    all_emails = set()
    
    try:
        print(f"[JS Rendering] Scraping base URL: {base_url}")
        
        # Process base URL
        html_content = js_render_with_session(base_url, headers)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            emails = extract_emails(soup)
            all_emails.update(emails)
            subpages = list(find_subpage_urls(soup, base_url))
            visited_urls.add(base_url)
            
            # Process subpages
            subpages_processed = 0
            for subpage in subpages:
                if subpage in visited_urls or subpages_processed >= max_subpages:
                    continue
                
                print(f"[JS Rendering] Scraping subpage: {subpage}")
                html_content = js_render_with_session(subpage, headers)
                if html_content:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    emails = extract_emails(soup)
                    all_emails.update(emails)
                    visited_urls.add(subpage)
                
                subpages_processed += 1
                time.sleep(random.uniform(1, 2))  # Polite delay between requests
    
    except Exception as e:
        print(f"Error during JS rendering scraping: {str(e)}")
    
    return ', '.join(all_emails) if all_emails else "No email ID found"

# Combined function that tries primary method first, then backup
def extract_emails_with_fallback(url):
    if pd.isna(url) or not isinstance(url, str) or url.strip() == "":
        return "Invalid URL"
    
    # Clean the URL (remove trailing slashes, etc.)
    url = url.strip().rstrip('/')
    
    # Try primary method first
    primary_result = extract_emails_from_url(url)
    if primary_result:
        print(f"Found emails using primary method for {url}: {primary_result}")
        return primary_result
    
    # If primary method fails, try JS rendering method
    print(f"Primary method found no emails for {url}, trying JS rendering method...")
    js_result = find_emails_js(url)
    print(f"JS rendering method results for {url}: {js_result}")
    return js_result

def process_single_url(url):
    """Process a single URL with both methods, to be used with ThreadPoolExecutor"""
    try:
        return extract_emails_with_fallback(url)
    except Exception as e:
        print(f"Error processing URL {url}: {str(e)}")
        return f"Error: {str(e)}"

def get_optimal_workers(file_size):
    # Reduced number of workers since JS rendering is resource-intensive
    if file_size <= 50:
        return 3  
    elif file_size <= 100:
        return 5
    else:
        return 8

def process_urls_in_parallel(df, url_column, num_workers):
    urls = df[url_column].tolist()
    results = []
    
    # Process in smaller batches to manage resources better
    batch_size = 10
    for i in range(0, len(urls), batch_size):
        batch = urls[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            batch_results = list(executor.map(process_single_url, batch))
        results.extend(batch_results)
        time.sleep(1)  # Brief pause between batches
    
    return results

@app.route('/')
def upload_file():
    return render_template('upload.html')

@app.route('/process', methods=['POST'])
def process_file():    
    file = request.files['file']
    if not file or not file.filename.endswith('.xlsx'):
        return "Invalid file type. Please upload an Excel file.", 400
    try:
        df = pd.read_excel(file)
        url_column = find_url_column(df.columns)
        if not url_column:
            return "No column found that likely contains URLs.", 400
        
        num_workers = get_optimal_workers(len(df))
        df['Emails'] = process_urls_in_parallel(df, url_column, num_workers)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        original_filename = file.filename
        processed_filename = f"{original_filename}"
        return send_file(output, as_attachment=True, download_name=processed_filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return f"An error occurred: {e}", 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)