
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from helpers import mf_struct_from_input_file,mf_struct_from_user_input,print_dict_as_table,create_bitmaps,extract_rows_bitmap,parse_condition,parse_where_condition,parse_having_condition

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
    input_file_no = 0
    mf_struct = {}
    option = input("Choose: \n1 to input arguments manually.\n2 to select input file.\n")
    while option != '1' and option != '2':
        option = input('Invalid option. Please try again:\nChoose: \n1 to input arguments manually.\n2 to select input file.\n')
    if option == '1':
        mf_struct = mf_struct_from_user_input()
    elif option == '2':
        input_file_no = int(input("Input number for input file: "))
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

    # handle normal MF query
    if mf_struct['n'] == 0:

        # Iterate over each group key and bitmap
        for index, (group_key, bitmap) in enumerate(bitmaps.items()):
            # Initialize group entry with group key values
            group_entry = {attr: val for attr, val in zip(mf_struct['V'], group_key)}
            relevant_rows = extract_rows_bitmap(bitmap, _all_sales)  # Extract relevant rows
            
            # where clause
            if mf_struct['W'] != '-':
                parsed_where_condition = parse_where_condition(mf_struct['W'])
                relevant_where_rows = []
                for row in relevant_rows:
                    if eval(parsed_where_condition):
                        relevant_where_rows.append(row)
               


            # Initialize aggregation columns
            for column in mf_struct['S']:
                if column not in mf_struct['V']:
                    group_entry[column] = None

            # Perform aggregations
            for column in mf_struct['S']:
                if column not in mf_struct['V']:
                    agg_header = column
                    split_values = agg_header.split('_')
                    agg, col = split_values
                    if mf_struct['W'] != '-':
                        # Collect values for the aggregation
                        values = [row[col] for row in relevant_where_rows if col in row]

                        if agg == 'sum':
                            group_entry[agg_header] = sum(values)

                        elif agg == 'avg':
                            group_entry[agg_header] = sum(values) / len(values) if values else 0

                        elif agg == 'min':
                            group_entry[agg_header] = min(values) if values else None

                        elif agg == 'max':
                            group_entry[agg_header] = max(values) if values else None

                        elif agg == 'count':
                            group_entry[agg_header] = len(relevant_where_rows)
                    else: 
                        # Collect values for the aggregation
                        values = [row[col] for row in relevant_rows if col in row]

                        if agg == 'sum':
                            group_entry[agg_header] = sum(values)

                        elif agg == 'avg':
                            group_entry[agg_header] = sum(values) / len(values) if values else 0

                        elif agg == 'min':
                            group_entry[agg_header] = min(values) if values else None

                        elif agg == 'max':
                            group_entry[agg_header] = max(values) if values else None

                        elif agg == 'count':
                            group_entry[agg_header] = len(relevant_rows)

            # Append the final group entry to _global
            _global.append(group_entry)          
    else:
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
                        # print(parsed_condition)
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
      
    # FINAL todo

    # TESTING try with other grouping attributes
    # CODE migrate all this to generator
    # FEATURE add user input capablilty (DONE)
    # bug : 'date' datatype , 'int' datatypes dont aggregate  , make separate parsing conditions for string, date, and int, change parse_condition, parse_where_condition, parse_having_condition , and test all input files
    # make PPT

    # having clause processing
    if 'G' in mf_struct.keys():
        parsed_having_condition = parse_having_condition(mf_struct['G'][0])
        temp_global = []
        for row in _global:
            if  eval(parsed_having_condition):
                temp_global.append(row)
        if len(temp_global) >= 1:
            _global = temp_global   
        else:
            _global = [{col_header: None for col_header in mf_struct['S']}]

    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    