import csvio

# Input file path (local). Change this based on what the name of your input file is.
input_csv_path = "files/scrapetest.csv"
# Output file path (local). Change this based on what you want the output file to be named.
output_csv_path = "files/output.csv"

csvio.process_csv(input_csv_path, output_csv_path)

exit(0)
