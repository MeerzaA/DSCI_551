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
    file_handles = [open(run_file, "r") for run_file in run_files]

    page1 = [next(fh, None) for fh in file_handles]
    page2 = [next(fh, None) for fh in file_handles]
    page3 = []

    #No .lower() so sorting is similar to SQL order by. if Bannana vs apple than Uppercase would come first: [Bannana,apple] case-Insensative
    with open(output_filename, "w", newline="") as output_file:
        while any(page1) or any(page2):
            #Compare the first row from each page
            min_row = min(filter(None, page1 + page2), key=lambda row: row.split(',')[0])
            page3.append(min_row)  
            
            #Update the page containing the min_row
            if min_row in page1:
                page1 = [next(fh, None) if row == min_row else row for fh, row in zip(file_handles, page1)]
            else:
                page2 = [next(fh, None) if row == min_row else row for fh, row in zip(file_handles, page2)]

            while len(page3) >= 2: #we write to disk as soon as we have atleast 2 rows
                output_file.write(page3.pop(0))

        if page3:
            output_file.write(page3[0])

    #handles and delete temp files
    for fh, run_file in zip(file_handles, run_files):
        fh.close()
        os.remove(run_file)

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
    
    #Merge sorted runs into the final output file
    merge_runs(run_files, output_filename)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python3 ext_sort.py input.csv output.csv")
    else:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]
        external_sort(input_filename, output_filename)
