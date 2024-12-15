
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from helpers import mf_struct_from_input_file,main_algoritm,print_dict_as_table,create_bitmaps,extract_rows_bitmap,parse_condition,parse_where_condition

# THIS IS A FILE FOR TESTING PURPOSES, PLEASE DELETE AT THE END OF PROJECT

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    _global = []


    # take user input
    # input_file_no = 0
    # mf_struct = {}
    # option = input("Choose 1 to input arguments manually\nChoose 2 to select input file")
    # if int(option) == 1:
    #     # insert code for parsing arguments manually
    #     while True:
    #         pass
    # else:
    #     # code for reading from input file
    #     pass
    
    input_file_no = 5
    mf_struct = mf_struct_from_input_file(input_file_no)
    print(mf_struct)
    
    # get all the rows from table
    rows = cur.fetchall()

    _all_sales = []

    # add each row to  _all_sales
    for row in rows:
         _all_sales.append(dict(row))
    # STEP 1
    '''
    Partition R according to the grouping attributes into buckets. Read in a bucket and process each group. If a group has size g, then create n bitmaps b_1, ..., b_n of length g. The bitmaps encode the underlying R_i relations in the definition of Φ^n (on the current group). We process each group according to the following steps.
    '''
    bitmaps = create_bitmaps(_all_sales,mf_struct['V'])
    for group_key, bitmap in bitmaps.items():
        bitmaps_rows = extract_rows_bitmap(bitmap, _all_sales)
        '''
        # this builds the 1st scan
        if only 1 group attribute
            append the bitmaps of group key to _global
            add group key to group by attr column
        else
            preprocess the tuple
            map according to each column of group by attribute array
        '''
        # print_dict_as_table(bitmaps_rows)  
    # STEP 2
    '''
    Make arrays for each grouping variable and calculate the aggregates 
    '''
    if mf_struct['W']:
        where_condition = parse_where_condition(mf_struct['W'])
    # global_aggregates = [[] for _ in range(mf_struct['n'])]
    for index,(group_key, bitmap) in enumerate(bitmaps.items()):

        group_entry = {attr: val for attr, val in zip(mf_struct['V'], group_key)}
        group_entry.update({agg: [] for agg in mf_struct['F']})  # Initialize aggregates as empty lists
        
        local_aggregate_rows = [[] for _ in range(mf_struct['n'])]
        relevant_rows = extract_rows_bitmap(bitmap, _all_sales)
        for row in relevant_rows:
                for index, condition in enumerate(mf_struct['C']):
                # Parse the condition dynamically
                    
                    parsed_condition = parse_condition(condition, group_key, mf_struct['V'])
                    if where_condition != '-':
                        full_condition = f"{parsed_condition} and {where_condition}"
                    else:
                        full_condition = parsed_condition    
            
                    if eval(full_condition):
                        local_aggregate_rows[index].append(row)              

        for i, local_rows in enumerate(local_aggregate_rows):   

            agg_header = mf_struct['F'][i]
            split_values = agg_header.split('_')
            agg, gv, col = split_values
            if agg == 'sum':
                # Sum of 'col' values
                total_value = sum(row[col] for row in local_rows if col in row)
                # print(f"Sum of {col} for {agg}_{gv}_{col}: {total_value}")
                # global_aggregates[int(gv) - 1].append(total_value)
                group_entry[agg_header] = total_value

            elif agg == 'avg':
                # Average of 'col' values
                total_value = sum(row[col] for row in local_rows if col in row)
                average_value = total_value / len(local_rows) if local_rows else 0
                # print(f"Average of {col} for {agg}_{gv}_{col}: {average_value:.2f}")
                # global_aggregates[int(gv) - 1].append(average_value)
                group_entry[agg_header] = average_value

            elif agg == 'min':
                # Minimum of 'col' values
                min_value = min(row[col] for row in local_rows if col in row)
                # print(f"Min of {col} for {agg}_{gv}_{col}: {min_value}")
                # global_aggregates[int(gv) - 1].append(min_value)
                group_entry[agg_header] = min_value

            elif agg == 'max':
                # Maximum of 'col' values
                max_value = max(row[col] for row in local_rows if col in row)
                # print(f"Max of {col} for {agg}_{gv}_{col}: {max_value}")
                # global_aggregates[int(gv) - 1].append(max_value)
                group_entry[agg_header] = max_value
                

            elif agg == 'count':
                # Count of rows
                count = len(local_rows)
                # print(f"Count of rows for {agg}_{gv}_{col}: {count}")
                # global_aggregates[int(gv) - 1].append(count)  
                group_entry[agg_header] = count    
        
        _global.append(group_entry)          

    # STEPS TO DO
    # make local_quant_rows & global_aggregates general made off n? (DONE)
    # make a function to extract aggregates input (mf_struct['F'], local_quant_rows) Output: calculates and appends aggregate to global_aggregates , switch case? (DONE)
    # make sure parse_condition is general (DONE)
    # mapping distict customers and the global_aggregates to the _global column IMP!!!!! (DONE)

    # LATER TO DO
    # TESTING try with having, where (DONE), other grouping arributes, no such that
    # migrate all this to generator 
    # add user input capablilty

    # FINAL TODOS
    # make sure parse_condition is general
    # TESTING try with having,
    # migrate all this to generator 
    # add user input capablilty

    # PPT
    # make ppt
    

    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    