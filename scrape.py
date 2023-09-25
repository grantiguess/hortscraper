import requests
from bs4 import BeautifulSoup
import re
from googlesearch import search
from urllib.parse import urljoin
import time


def get_url_from_name(company_name):
    # Search for company name on Google and get first five results.
    query = f"\"{company_name}\" contact"
    search_results = search(query, num_results=5)
    return search_results


def emails_from_url(url, visited_urls=None):
    if visited_urls is None:
        visited_urls = set()

    subpages = ["contact", "about", "contact-us", "about-us",
                "contact.html", "about.html", "contact-us.html", "about-us.html"]

    try:
        # Introduce a small delay to avoid overloading servers
        time.sleep(0.01)

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

        # Find email addresses using a regular expression
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,3}\b')
        emails = email_pattern.findall(soup.get_text())

        # If emails are found, return them
        if emails:
            print("\n~~~ 1 or More Emails found! ~~~\n")
            return ' '.join(emails)

        # Search for emails in subpages
        for subpage in subpages:
            subpage_url = urljoin(url, subpage)
            # Search for an explicit port number in a URL and remove it
            if ':' in subpage_url:
                subpage_url = subpage_url.replace(':443', '')  # Replace ':443' with an empty string

            # Check if we've visited this URL before to avoid infinite loops
            if subpage_url not in visited_urls:
                visited_urls.add(subpage_url)
                subpage_emails = emails_from_url(subpage_url, visited_urls)
                if subpage_emails and subpage_emails != ' ':
                    return subpage_emails

        return None

    except requests.exceptions.RequestException as e:
        print(f"Error requesting URL: {e}")
        return None


def email_from_name(company):
    # URL of the page where you want to start scraping emails
    urls = get_url_from_name(company)
    # Call the function for every and check if no emails were found
    for site in urls:
        if emails_from_url(site):
            print(site)
            return
    print("No emails were found when 5 sites were indexed")
