import csv
import os

def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.
    
    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.

    """
    sortedRows = sorted(chunk, key=lambda row: row[0]) #On Piazza they said to not lower()
    tempf = f"temp{run_id}.csv"
    with open(tempf, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(sortedRows)

    return tempf

def merge_runs(run_files, output_filename):
    """
    Merges sorted files (runs) into a single sorted output file.
    
    :param run_files: List of filenames representing sorted runs to be merged.
    :param output_filename: Filename for the merged, sorted output.

    """
    runFilesList = []

    for f in run_files:
        runFilesList.append(open(f, 'r'))

    csv_readers = [csv.reader(f) for f in runFilesList]

    current_rows = [[next(reader, None) for _ in range(2)] for reader in csv_readers] #tasked to only compare 2 rows at most
    print(current_rows)

    # No .lower() so sorting is similar to SQL order by. if Bannana vs apple than Uppercase would come first: [Bannana,apple] case-Insensative
    with open(output_filename, 'w', newline='') as f:
        writer = csv.writer(f)

        while any(rows[0] is not None for rows in current_rows):

            min_index = min((i for i, rows in enumerate(current_rows) if rows[0] is not None), key=lambda i: current_rows[i][0][0]) 
            print(min_index)

            writer.writerow(current_rows[min_index].pop(0)) 

            if len(current_rows[min_index]) == 0: #row are immdetily replaced after being compared
                current_rows[min_index] = [next(csv_readers[min_index], None) for _ in range(2)] 

    for f in runFilesList:
        f.close()

    for f in run_files:
        os.remove(f)


def external_sort(input_filename, output_filename):
    """
    the external sort process: chunking, sorting, and merging.
    
    :param input_filename: Name of the file with data to sort.
    :param output_filename: Name of the file where sorted data will be written.

    """
    run_id = 0
    run_files = []
    
    with open(input_filename, "r") as f:
        chunk = []
        for line in f:
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
