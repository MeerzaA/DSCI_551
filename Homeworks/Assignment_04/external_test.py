import csv
import os

def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.
    
    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.
    """

    sorted_chunk = sorted(chunk, key=lambda row: row[0]) #On Piazza they said not to do lowercase()
     
   
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
    while len(run_files) > 1:
        new_run_files = []

        for i in range(0, len(run_files), 2):
            if i + 1 < len(run_files):
                output_file = f"temp_merged_{i//2}.csv"
                new_run_files.append(output_file)

                with open(run_files[i], 'r') as file1, open(run_files[i+1], 'r') as file2, open(output_file, 'w', newline='') as output_file:
                    reader1 = csv.reader(file1)
                    reader2 = csv.reader(file2)
                    writer = csv.writer(output_file)

                    row1 = next(reader1, None)
                    row2 = next(reader2, None)

                    while row1 is not None and row2 is not None:
                        if row1[0] < row2[0]:
                            writer.writerow(row1)
                            row1 = next(reader1, None)
                        else:
                            writer.writerow(row2)
                            row2 = next(reader2, None)

                    while row1 is not None:
                        writer.writerow(row1)
                        row1 = next(reader1, None)

                    while row2 is not None:
                        writer.writerow(row2)
                        row2 = next(reader2, None)

                # Delete the input run files as they are no longer needed
                os.remove(run_files[i])
                os.remove(run_files[i+1])

        run_files = new_run_files

    # Rename the last remaining run file to the desired output filename
    os.rename(run_files[0], output_filename)

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
