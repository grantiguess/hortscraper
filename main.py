import csvio
import scrape

# Test input file path (local) for ~200 line test file.
test_csv_path = "files/scrape_name_test.csv"

# Input file path (local). Change this based on what the name of your input file is.
input_csv_path = "files/names_full.csv"
# Output file path (local). Change this based on what you want the output file to be named.
output_csv_path = "files/names_6.csv"

# Whether we are testing by company name
test = False


def define_inputs():
    print("Specify files? [Y/N]")
    res = input()
    if "Y" or "y" in res:
        files = []
        print("Enter input file name (no need to put .csv):")
        infile = input()
        files.append("files/" + infile + ".csv")
        print("Enter output file name no need to put .csv):")
        out = input()
        files.append("files/" + out + ".csv")
        return files
    else:
        print("Running program based off of input_csv_path and output_csv_path")


# Either process a CSV or run a test for a single company name.
print("How are you scraping?\n[U] From URLs, [N] From Names, [S] From Search. [T] for Testing function.")
result = input()
if result == "U" or result == "u":
    filenames = define_inputs()
    input_csv_path = filenames[0]
    output_csv_path = filenames[1]
    print("in: " + input_csv_path)
    print("out: " + output_csv_path)

    print("Enter the index to start processing (1 for the first row after the header): ")
    start_index = int(input()) - 1
    csvio.process_csv_urls(start_index, input_csv_path, output_csv_path)

elif result == "N" or result == "n":
    filenames = define_inputs()
    input_csv_path = filenames[0]
    output_csv_path = filenames[1]
    print("in: " + input_csv_path)
    print("out: " + output_csv_path)

    # Prompt the user for the starting index
    print("Enter the index to start processing (1 for the first row after the header): ")
    start_index = int(input()) - 1
    csvio.process_csv_names_ny(start_index, input_csv_path, output_csv_path)

elif result == "S" or result == "s":
    print("Enter output file name (ending in .csv):")
    outfile = input()
    outfile = "files/" + outfile + ".csv"
    print("What's the starting city index? (out of 1000)")
    start_idx = int(input())
    print("What's the ending city index? (out of 1000)")
    end_idx = int(input())
    csvio.process_searches(outfile, start_idx, end_idx)

elif result == "T" or result == "t":
    print("Testing mode.")
    count = 0
    scrape.copy_emails("files/ny_emails_merge.csv")
    # while True:
    #     print("Enter company name.")
    #     name = input()
    #     scrape.email_from_name(name)
    #     # scrape.emails_from_keyword()
    #
    #     print("Done with " + name + "!\n")

else:
    print("Invalid result. Please enter U, N, S, or T.")


print("Program has completed.")
exit(0)
