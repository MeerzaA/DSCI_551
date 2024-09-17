import csv


def simple_sort_based_join(product_filename, maker_filename, output_filename):
    """
    Executes a simple sort-based join between two datasets and writes the output to a CSV file.

    :param product_filename: Filename of the sorted product dataset.
    :param maker_filename: Filename of the sorted maker dataset.
    :param output_filename: Filename for the output joined dataset.

    """
    #I am confused about Q2 saying that this func needs to do sort operation using ext_sort.py 
    #On Piazza it was mentioned that we run ext_sort.py seperately on product and maker and than run the sorted csv files here
    #So that is why I excluded the step for sorting in the pdf the excaute format is discribed as :
    #python3 ssb_join.py product_sorted.csv maker_sorted.csv joined_data.csv | meaning the tables sorted already.
    #I hope that is okay 
    
    with open(product_filename, 'r') as sorted_product, open(maker_filename, 'r') as sorted_maker:

        p_read = csv.reader(sorted_product)
        m_reader = csv.reader(sorted_maker)

        p_line = next(p_read, None) # buffer1
        m_line = next(m_reader, None) # buffer2 

        with open(output_filename, 'w', newline='') as f:
            
            writer = csv.writer(f)

            while p_line is not None and m_line is not None:
                
                if p_line[0] == m_line[0]: #consider this buffer3
                
                    writer.writerow(p_line + m_line[1:]) #its than immeditely written to joined_data file 
                    p_line = next(p_read, None)
                    m_line = next(m_reader, None)
                
                elif p_line[0] < m_line[0]:
                
                    p_line = next(p_read, None) #immedietly replace buffer1 after match attempt 
                
                else:
                
                    m_line = next(m_reader, None) #immedietly replace buffer2 after match attempt 

# python  ssb_join.py product_sorted.csv maker_sorted.csv joined_data.csv
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python ssb_join.py <product_file.csv> <maker_file.csv> <output_file.csv>")
    else:
        simple_sort_based_join(sys.argv[1], sys.argv[2], sys.argv[3])
