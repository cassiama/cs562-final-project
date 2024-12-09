
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from helpers import mf_struct_from_input_file,main_algoritm,print_dict_as_table,create_bitmaps,extract_rows_bitmap

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
    
    input_file_no = 2
    mf_struct = mf_struct_from_input_file(input_file_no)
   
    algoritm = main_algoritm(mf_struct)
    print(mf_struct)

    # Initialize _global with empty arrays for each column name in 'S'
    _global = [{key: [] for key in mf_struct['V'] + mf_struct['F']}]
    print(_global)
  
    
    # get all the rows from table
    rows = cur.fetchall()

    _all_sales = []

    # add each row to  _all_sales
    for row in rows:
         _all_sales.append(dict(row)) 
        #  _global.append(dict(row)) 
    # STEP 1
    '''
    Partition R according to the grouping attributes into buckets. Read in a bucket and process each group. If a group has size g, then create n bitmaps b_1, ..., b_n of length g. The bitmaps encode the underlying R_i relations in the definition of Φ^n (on the current group). We process each group according to the following steps.
    '''
    bitmaps = create_bitmaps(_all_sales,mf_struct['V'])
    for group_key, bitmap in bitmaps.items():
        bitmaps_rows = extract_rows_bitmap(bitmap, _all_sales)
        print(f"Group: {group_key}")
        '''
        # this builds the 1st scan
        if only 1 group attribute
            append the bitmaps of group key to _global
            add group key to group by attr column
        else
            preprocess the tuple
            map according to each column of group by attribute array
        '''
        print_dict_as_table(bitmaps_rows)  
    # STEP 2
    '''
    Make (n + 1) passes over the group. On the first pass (i = 0), the aggregates F_o of the full group are computed. On subsequent passes 1 <= i <= n, the selections σ_i on r_i and the aggregates F_i of r_i are simultaneously computed. The selection result is stored in b_i, with a bit set if the corresponding tuple (together with previously computed aggregate values) satisfies σ_i. If any selection result is empty, we can immediately move to the next group.
    '''
    # for _ in range(1, len(mf_struct['n'])):
    #     print("Calculating the 1st aggregate on the group")
    #     for i in range(len(bitmaps)):
    #         pass
    
    # STEP 3
    '''
    Because of the form of G, each G_j can be checked either (a) on the aggregate tables alone, or (b) on a single R_i relation together with the aggregate tables. For a group, there is a single tuple in each aggregate table. Hence selection conditions on the aggregate tables either include or exclude the group as a whole; if such a condition is violated, we simply move to the next group.
    Conditions that mention an attribute from R_i are processed by making an additional pass, further restricting the bitmap b_i. Again, if the resulting bitmap is all zeroes we immediately move to the next group. We make at most m additional passes over the group.
    '''

    # STEP 4
    '''
    We now compose the join (according to the grouping attributes) of all of the R_i relations as indicated by their bitmaps, together with the aggregate values for the R_i S. We project onto the attributes in S as we compute the join.
    '''

    # print_dict_as_table(_all_sales) 
    
    # using a for loop, the mf_struct and algoritm to generate the output
    for index, (cust, prod, day, month, year, state, quantC, date) in enumerate(_all_sales, 1):
        {algoritm}
    
    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    