import csv
import json
from urllib.parse import urlencode, urlparse, parse_qs, unquote
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import urllib
from validate_email import validate_email

# The number of search results the program retrieves from Google
searches = 3

# API key for scrapeops.io
key = "14f750ad-d979-431b-a483-3397768380eb"

# Define the list of websites to exclude
excluded_sites = ["yelp", "yellowpages.com", "mapquest.com", "google.com", "amazon.com", "pinterest.com",
                  "linkedin.com", "indeed.com", "facebook.com", "instagram.com", "homedepot", "angi",
                  "shopify.com", "zillow.com", "realtor.com", "youtube.com", "superpages.com", "bbb.org", "yahoo"]


def search(query, num_results):
    results = []
    query_formatted = urllib.parse.quote_plus(query)

    search_url = ("https://www.google.com/search?q=" + query_formatted)
    print('Searching: ' + search_url)

    try:
        proxy_params = {
            'api_key': key,  # Make sure you have your API key
            'url': search_url,
            'auto_extract': 'google',
        }

        page = requests.get(
            url='https://proxy.scrapeops.io/v1/',
            params=urllib.parse.urlencode(proxy_params),
            timeout=120,
        )
        page.raise_for_status()  # Check for HTTP errors
        page_json = page.json()
        # print(json.dumps(page_json, indent=4))

        for item in page_json['data']['organic_results']:
            link = item['link']
            # Check if the link contains any excluded domain
            if not any(site in link for site in excluded_sites) and len(results) < num_results:
                results.append(link)

    except requests.exceptions.RequestException as e:
        raise Exception(f"An error occurred: {e}")

    return results


def extract_emails(text):
    # Use a regular expression for email extraction
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,3}\b')
    emails = email_pattern.findall(text)

    # Validate and filter out invalid emails
    valid_emails = [email for email in emails if validate_email(email, verify=False)]

    return valid_emails


# Removes phone numbers from emails
def remove_phone_numbers_from_emails(emails, phone_numbers):
    # Remove characters to the left of phone numbers from each email
    cleaned_emails = []
    for email in emails:
        cleaned_email = email
        for phone_number in phone_numbers:
            cleaned_email = re.sub(f'.*?{re.escape(phone_number)}', '', cleaned_email)
        cleaned_emails.append(cleaned_email)
    return cleaned_emails


# Uses beautifulsoup and requests to parse through the HTML of a site and search for strings in an email format
def emails_from_url(url, visited_urls=None):
    if any(site in url for site in excluded_sites):
        print("Excluded site, skipping...")
        return None

    if visited_urls is None:
        visited_urls = set()

    subpages = ["contact", "about", "contact-us", "about-us"]

    try:
        # Search for an explicit port number in a URL and remove it
        if ':' in url:
            url = url.replace(':443', '')  # Replace ':443' with an empty string

        # Send an HTTP GET request with allow_redirects=True to follow redirects
        response = requests.get(url, headers={'User-agent': 'cbl'}, allow_redirects=True)

        # Check if the request was successful
        response.raise_for_status()

        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        print("Scraping emails for: " + url)

        # Extract text from the HTML page
        page_text = soup.get_text()

        # Extract emails from the page text
        emails = extract_emails(page_text)

        # Extract phone numbers from the page text
        phone_numbers = re.findall(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', page_text)

        # Remove phone numbers from each email
        cleaned_emails = remove_phone_numbers_from_emails(emails, phone_numbers)

        # Parse list of emails into a set (removes duplicates) and then back into a list
        cleaned_emails = list(set(cleaned_emails))

        # If cleaned emails are found, return them
        if cleaned_emails:
            print("\n~~~ 1 or More Emails found! ~~~\n")
            return cleaned_emails

        # Search for emails in subpages
        for subpage in subpages:
            subpage_url = urllib.parse.urljoin(url, subpage)
            # Search for an explicit port number in a URL and remove it
            if ':' in subpage_url:
                subpage_url = subpage_url.replace(':443', '')  # Replace ':443' with an empty string

            # Check if we've visited this URL before to avoid infinite loops
            if subpage_url not in visited_urls:
                visited_urls.add(subpage_url)
                subpage_emails = emails_from_url(subpage_url, visited_urls)
                if subpage_emails and subpage_emails != ' ':
                    clean_sub = list(set(remove_phone_numbers_from_emails(subpage_emails, phone_numbers)))
                    return clean_sub

        return None

    except requests.exceptions.RequestException as e:
        print(f"Error requesting URL: {e}")
        return None


# Takes a company name and uses search(name) to get URLs, then uses emails_from_url(site) to find emails
def email_from_name(company):
    try:
        query = f"\"{company}\" " \
                f"AND (plant OR greenhouse OR nursery OR flower " \
                f"OR garden OR flower OR floral)"

        # URL of the page where you want to start scraping emails
        urls = search(query, 5)
        # Call the function for every and check if no emails were found
        for site in urls:
            emails = emails_from_url(site)
            if emails:
                print(site)
                return emails

        print("No emails were found for " + company)
        return

    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        return False


# Takes a company name and uses search(name) to get URLs, then uses emails_from_url(site) to find emails
def email_from_name_ny(company, city):
    try:
        query = f"\"{company}\" {city}" \
                f"AND (plant OR greenhouse OR nursery OR flower " \
                f"OR garden OR flower OR floral)"

        # URL of the page where you want to start scraping emails
        urls = search(query, 5)
        # Call the function for every and check if no emails were found
        for site in urls:
            emails = emails_from_url(site)
            if emails:
                print(site)
                return emails

        print("No emails were found for " + company)
        return

    except Exception as e:
        print(f"An exception occurred: {str(e)}")
        return False


def write_to_csv(output_csv_path, result):
    with open(output_csv_path, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(result)


def emails_from_keyword_helper(output_csv_path, url):
    try:
        print("Searching " + url)
        proxy_params = {
            'api_key': key,  # Make sure you have your API key
            'url': url,
            'auto_extract': 'google',
        }

        page = requests.get(
            url='https://proxy.scrapeops.io/v1/',
            params=urllib.parse.urlencode(proxy_params),
            timeout=120,
        )
        page.raise_for_status()  # Check for HTTP errors
        page_json = page.json()
        # print(json.dumps(page_json, indent=4))

        if 'data' in page_json and 'organic_results' in page_json['data']:
            for item in page_json['data']['organic_results']:
                link = item.get('link', '')
                name = item.get('title', '')
                if not link or not name:
                    continue
                result = [name, link]
                # Check if the link contains any excluded domain
                if not any(site in link for site in excluded_sites):
                    write_to_csv(output_csv_path, result)  # Write to CSV dynamically
        else:
            print("No data from this search")

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")


def emails_from_keyword(output_csv_path, city, num_pages=30):
    for page in range(1, num_pages + 1):
        query = "greenhouse AND flower AND US AND nursery AND " + "\"" + city + "\""
        query_formatted = urllib.parse.quote_plus(query)
        search_url = f"https://www.google.com/search?q={query_formatted}&start={(page - 1) * 10}"
        emails_from_keyword_helper(output_csv_path, search_url)


def scrape_ny():
    # Fetch JSON data from the link
    url = 'https://data.ny.gov/resource/qke7-n4w8.json'
    response = requests.get(url)
    data = json.loads(response.text)

    # Define CSV file and write header
    csv_file = 'files/nurseries_data.csv'
    header = ['Name', 'Phone Number', 'Address']

    with open(csv_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header)

        # Iterate through each nursery in the JSON data
        for nursery in data:
            name = nursery.get('trade_name', '')
            phone_number = nursery.get('business_phone', '')
            address = f"{nursery.get('street_address_number', '')} {nursery.get('street_name', '')}, {nursery.get('city', '')}, {nursery.get('state', '')} {nursery.get('zip_code', '')}"

            # Write nursery details to CSV
            csv_writer.writerow([name, phone_number, address])

    print(f'Data has been successfully written to {csv_file}.')


def copy_emails(csv_path):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_path)

    # Group emails by name and concatenate them with space
    email_group = df.groupby('Name')['Email'].agg(lambda x: ' '.join(x)).reset_index()

    # Merge the grouped emails with the original DataFrame based on the 'Name' column
    df_merged = pd.merge(df, email_group, how='left', on='Name')

    # Rename the new column with concatenated emails to 'Emails'
    df_merged.rename(columns={'Email_y': 'Emails'}, inplace=True)

    # Drop the original 'Email' column and any duplicate rows
    df_merged.drop(['Email_x'], axis=1, inplace=True)
    df_merged.drop_duplicates(inplace=True)

    # Save the updated DataFrame to a new CSV file
    df_merged.to_csv('ny_out.csv', index=False)
