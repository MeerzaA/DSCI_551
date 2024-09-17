import csv
import os

def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.
    
    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.
    """
    # Sort the chunk by the first column (name)
    sorted_chunk = sorted(chunk, key=lambda row: row[0]) #On Piazza they said to not lowercase()
    
    # Write the sorted chunk to a temporary file
    temp_filename = f"temp_run_{run_id}.csv"
    with open(temp_filename, "w", newline="") as temp_file:
        writer = csv.writer(temp_file)
        writer.writerows(sorted_chunk)

    return temp_filename

def merge_runs(run_files, output_filename):
    """
    Merges sorted files (runs) into a single sorted output file.
    
    :param run_files: List of filenames representing sorted runs to be merged.
    :param output_filename: Filename for the merged, sorted output.
    """
    # If there's only one run file left, rename it to the final output file and return
    if len(run_files) == 1:
        os.rename(run_files[0], output_filename)
        return

    # Initialize a list to store the file objects
    file_objects = []

    # Open all the run files and store the file objects in the list
    for run_file in run_files:
        file_objects.append(open(run_file, 'r'))

    # Initialize a list to store the csv reader objects
    csv_readers = [csv.reader(f) for f in file_objects]

    # Initialize a list to store the current rows of each csv reader
    current_rows = [[next(reader, None) for _ in range(2)] for reader in csv_readers]

    # Open the output file
    with open(output_filename, 'w', newline='') as f:
        writer = csv.writer(f)

        # While there are still rows left to process
        while any(rows[0] is not None for rows in current_rows):
            # Find the index of the smallest row
            min_index = min(
                (i for i, rows in enumerate(current_rows) if rows[0] is not None),
                key=lambda i: current_rows[i][0][0]
            )

            # Write the smallest row to the output file
            writer.writerow(current_rows[min_index].pop(0))

            # If we've used up all the rows from this reader, fetch the next two rows
            if len(current_rows[min_index]) == 0:
                current_rows[min_index] = [next(csv_readers[min_index], None) for _ in range(2)]

    # Close all the run files
    for f in file_objects:
        f.close()

    # Delete all the run files
    for run_file in run_files:
        os.remove(run_file)


def external_sort(input_filename, output_filename):
    """
    the external sort process: chunking, sorting, and merging.
    
    :param input_filename: Name of the file with data to sort.
    :param output_filename: Name of the file where sorted data will be written.
    """
    chunk = []
    run_id = 0
    run_files = []
    
    with open(input_filename, "r") as input_file:
        for line in input_file:
            row = line.strip().split(",")
            chunk.append(row)
            if len(chunk) == 2:
                temp_filename = sort_and_write_chunk(chunk, run_id)
                run_files.append(temp_filename)
                run_id += 1
                chunk.clear()
        if chunk:
            temp_filename = sort_and_write_chunk(chunk, run_id)
            run_files.append(temp_filename)
            chunk.clear()
    
    # Merge sorted runs into the final output file
    merge_runs(run_files, output_filename)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python3 ext_sort.py input.csv output.csv")
    else:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]
        external_sort(input_filename, output_filename)
