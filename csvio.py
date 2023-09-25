import csv
import scrape


def process_csv(input_csv_path, output_csv_path):
    # Print starting message
    print('Reading ' + input_csv_path + " and outputting to " + output_csv_path)
    # Open the input and output CSV files
    with open(input_csv_path, "r") as input_file, open(output_csv_path, "w", newline="") as output_file:
        # Create CSV readers and writers
        csv_reader = csv.reader(input_file)
        csv_writer = csv.writer(output_file)

        next(csv_reader)
        count = 1

        for row in csv_reader:
            print("Starting row " + str(count))
            # Check if there are at least 21 columns and if row[20] is not an empty string
            if len(row) > 20 and row[20].strip():
                # Get the company name from column 0
                company_name = row[0]

                # Get the company URL from column 20
                company_url = "http://" + row[20].strip()  # Add "http://" to the URL

                # Call the scrape_emails_from_url function
                scraped_emails = scrape.emails_from_url(company_url)

                # Append the results to a new row in the output CSV
                new_row = [company_name, company_url, scraped_emails]
                csv_writer.writerow(new_row)
                print("Finished row " + str(count) + '\n')
            else:
                print("No url for row " + str(count) + '\n')
            count += 1

    print("Output written to " + output_csv_path)
