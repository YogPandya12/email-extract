# import sys
# sys.path.append('./env/lib/site-packages')
from flask import Flask, request, render_template, send_file
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin
import os
import shutil

app = Flask(__name__)
application = app

def find_url_column(columns):
    keywords = ['website', 'url', 'websites', 'urls']
    for col in columns:
        if any(keyword in col.lower() for keyword in keywords):
            return col
    return None

def extract_emails_from_url(url):
    """Fetch emails from the given URL and explore potential internal links for emails."""
    if pd.isna(url) or not isinstance(url, str):
        return ""
    print("CKPT1: Starting URL Processing")
    if not url.startswith(('http://', 'https://')):
        url = f"http://{url}"
    print(f"CKPT2: Final URL -> {url}")
    visited_urls = set()
    emails = set()
    print("CKPT3: Initialization Complete")
    keywords = ['contact', 'about', 'get in touch', 'reach us', 'communication','contacts','about the company','contact us']
    
    base_domain = urlparse(url).netloc

    def is_internal_link(link):
        """Check if a link belongs to the same domain."""
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
            
            # Extract emails from the current page text
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
    return ', '.join(emails) if emails else "No email ID found"

def get_optimal_workers(file_size):
    """Return optimal number of workers based on the file size."""
    if file_size <= 100:
        return 5  
    elif file_size <= 300:
        return 10  
    else:
        return 20 

def process_urls_in_parallel(df, url_column, num_workers):
    """Process all URLs in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        return list(executor.map(extract_emails_from_url, df[url_column]))

SPLIT_FOLDER = 'split_processing'
os.makedirs(SPLIT_FOLDER, exist_ok=True)

def split_excel_file(file, chunk_size=150):
    """Splits the Excel file into smaller chunks if the size exceeds the limit."""
    df = pd.read_excel(file)
    file_name, _ = os.path.splitext(file.filename)
    split_files = []
    
    # Splitting the file into chunks
    for i, chunk in enumerate(range(0, len(df), chunk_size)):
        split_file_name = f"{SPLIT_FOLDER}/{file_name}_{i+1}.xlsx"
        df.iloc[chunk:chunk + chunk_size].to_excel(split_file_name, index=False)
        split_files.append(split_file_name)
    
    return split_files

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
        original_file_name, _ = os.path.splitext(file.filename)

        if len(df) > 150:
            split_files = split_excel_file(file, chunk_size=150)
        
        else:
            temp_file_path = f"{SPLIT_FOLDER}/{file.filename}"
            df.to_excel(temp_file_path, index=False)
            split_files = [temp_file_path]

        combined_df = pd.DataFrame()
        for split_file in split_files:
            df_split = pd.read_excel(split_file)
            url_column = find_url_column(df_split.columns)
            if not url_column:
                return f"No column found that likely contains URLs in {os.path.basename(split_file)}.", 400
            
            num_workers = get_optimal_workers(len(df_split))
            df_split['Emails'] = process_urls_in_parallel(df_split, url_column, num_workers)
            
            combined_df = pd.concat([combined_df, df_split], ignore_index=True)

        print('CKPT4 - OUTPUT FILE PROCESS')
        output_file_path = f"{SPLIT_FOLDER}/{original_file_name}.xlsx"
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            combined_df.to_excel(writer, index=False)

        response = send_file(output_file_path, as_attachment=True, download_name=f"{original_file_name}.xlsx",
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
        for filename in os.listdir(SPLIT_FOLDER):
            file_path = os.path.join(SPLIT_FOLDER, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

        return response
    except Exception as e:
        return f"An error occurred: {e}", 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)