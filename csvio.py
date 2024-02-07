import csv
import scrape
import concurrent.futures
import threading


def scrape_emails_from_url(url):
    return scrape.emails_from_url(url)


def process_csv_urls(start_index, input_csv_path, output_csv_path):
    # Open the input and output CSV files
    with open(input_csv_path, "r", encoding='utf-8') as input_file, open(output_csv_path, "w", newline="") as output_file:
        # Create CSV readers and writers
        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file)

        # Write header row for the output CSV
        csv_writer.writerow(['URL', 'Email'])

        # Skip rows up to the specified start_index
        for _ in range(start_index):
            next(csv_reader)

        count = start_index + 1

        # Define a threading lock for CSV writing
        csv_lock = threading.Lock()
        # Define a threading lock for printing messages
        print_lock = threading.Lock()

        def print_message(message):
            with print_lock:
                print(message)

        def process_row(row):
            nonlocal count
            try:
                print_message("Starting row " + str(count))
                if row[0].strip():
                    company_url = row[0].strip()

                    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                        future = executor.submit(scrape_emails_from_url, company_url)
                        scraped_emails = future.result(timeout=60)

                        if scraped_emails:
                            # Acquire the CSV lock before writing to the CSV file
                            with csv_lock:
                                for email in scraped_emails:
                                    csv_writer.writerow([company_url, email])

                            print_message("Finished row " + str(count) + '\n')
                        else:
                            print_message("No emails scraped for row " + str(count) + '\n')
                else:
                    print_message("No URL for row " + str(count) + '\n')
                count += 1
            except Exception as e:
                print_message(f"Error, skipping line {count}: {str(e)}\n")

        # Process each row in the CSV file using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executorr:
            futures = [executorr.submit(process_row, row) for row in csv_reader]
            concurrent.futures.wait(futures)

        # Ensure that the CSV file is closed and finalized even if the program is stopped early
        output_file.close()


# Code to scrape a custom CSV for emails that consists of 1 column of solely company names using email_from_name()
def process_csv_names(start_index, input_csv_path, output_csv_path):
    # Print starting message
    print('Reading ' + input_csv_path + " and outputting to " + output_csv_path)

    # Open the input and output CSV files
    with open(input_csv_path, "r") as input_file, open(output_csv_path, "w", newline="") as output_file:
        # Create CSV readers and writers
        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file)

        # Write header row for the output CSV
        csv_writer.writerow(['Company Name', 'Email'])

        # Skip rows up to the specified start_index
        for _ in range(start_index):
            next(csv_reader)

        count = start_index + 1

        # Define a threading lock for CSV writing
        csv_lock = threading.Lock()
        # Define a threading lock for printing messages
        print_lock = threading.Lock()

        def print_message(message):
            with print_lock:
                print(message)

        def process_row(row):
            nonlocal count
            try:
                print_message("Starting row " + str(count))
                if row[0].strip():
                    company_name = row[0].strip()

                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(scrape.email_from_name, company_name)
                        scraped_emails = future.result(timeout=30)

                        if scraped_emails:
                            # Acquire the CSV lock before writing to the CSV file
                            with csv_lock:
                                for email in scraped_emails:
                                    csv_writer.writerow([company_name, email])

                            print_message("Finished row " + str(count) + '\n')
                        else:
                            csv_writer.writerow([company_name])
                            print_message("No emails scraped for row " + str(count) + '\n')
                else:
                    print_message("No name for row " + str(count) + '\n')
                count += 1
            except Exception as e:
                print_message(f"Error, skipping line {count}: {str(e)}\n")

        # Process each row in the CSV file using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_row, row) for row in csv_reader]
            concurrent.futures.wait(futures)

    print("Output written to " + output_csv_path)


def process_csv_names_ny(start_index, input_csv_path, output_csv_path):
    # Print starting message
    print('Reading ' + input_csv_path + " and outputting to " + output_csv_path)

    # Open the input and output CSV files
    with open(input_csv_path, "r") as input_file, open(output_csv_path, "w", newline="") as output_file:
        # Create CSV readers and writers
        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file)

        # Write header row for the output CSV
        csv_writer.writerow(['Company Name', 'Emails'])  # Updated header to include 'Emails'

        # Skip rows up to the specified start_index
        for _ in range(start_index):
            next(csv_reader)

        count = start_index + 1

        # Define a threading lock for CSV writing
        csv_lock = threading.Lock()
        # Define a threading lock for printing messages
        print_lock = threading.Lock()

        def print_message(message):
            with print_lock:
                print(message)

        def process_row(row):
            nonlocal count
            try:
                print_message("Starting row " + str(count))
                if row[0].strip():
                    company_name = row[0].strip()

                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(scrape.email_from_name_ny, company_name, row[2].strip()[1:-1])
                        scraped_emails = future.result(timeout=30)

                        if scraped_emails:
                            # Acquire the CSV lock before writing to the CSV file
                            with csv_lock:
                                # Combine emails into a space-delimited string
                                emails_str = ' '.join(scraped_emails)
                                csv_writer.writerow([company_name, emails_str])

                            print_message("Finished row " + str(count) + '\n')
                        else:
                            csv_writer.writerow([company_name, ''])
                            print_message("No emails scraped for row " + str(count) + '\n')
                else:
                    print_message("No name for row " + str(count) + '\n')
                count += 1
            except Exception as e:
                print_message(f"Error, skipping line {count}: {str(e)}\n")

        # Process each row in the CSV file using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_row, row) for row in csv_reader]
            concurrent.futures.wait(futures)

    print("Output written to " + output_csv_path)


def process_searches(output_csv_path, start_index, end_index):
    print("Scraping urls from city " + str(start_index) + " to " + str(end_index) + " (0-idx) and outputting to "
          + output_csv_path)

    with open("files/states.csv", 'r') as file:
        reader = csv.reader(file)
        city_name = None

        for count, row in enumerate(reader):
            if start_index <= count <= end_index:
                print("Scraping index " + str(count))
                city_name = row[0].strip()
                scrape.emails_from_keyword(output_csv_path, city_name)

    print("Output written to " + output_csv_path)
