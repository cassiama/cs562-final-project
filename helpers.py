import tabulate
from collections import defaultdict

def mf_struct_from_input_file(input_file_no):

    filepath = f'./q{input_file_no}.txt'
    ere = []
    with open(filepath, "r") as f:
        ere = f.read().splitlines()

    mf_struct = {}
    # print(ere)
    n_gv = 0
    select_attr = group_attr = aggregates = select_conds = having_conds = []

    # get the select attributes
    select_attr = ere[ere.index("SELECT ATTRIBUTE(S):") + 1].split(", ")
    # print(f"S: {select_attr}")
    for attr in select_attr:
        mf_struct[attr] = []
    mf_struct['S'] = select_attr
    # uncomment line above if we decide to handle mf_struct differently

    # get the number of grouping variables
    n_gv = int(ere[ere.index("NUMBER OF GROUPING VARIABLES(n):") + 1])
    # print(f"n: {n_gv}")
    mf_struct['n'] = n_gv
    # uncomment line above if we decide to handle mf_struct differently

    # get the grouping attributes
    group_attr = ere[ere.index("GROUPING ATTRIBUTES(V):") + 1].split(', ')
    # print(f"V: {group_attr}")
    mf_struct['V'] = group_attr
    # uncomment line above if we decide to handle mf_struct differently

    # get the aggregates vector
    aggregates = ere[ere.index("F-VECT([F]):") + 1].split(', ')
    # print(f"F: {aggregates}")
    mf_struct['F'] = aggregates
    # uncomment line above if we decide to handle mf_struct differently

    # get the select condition vector
    select_conds = ere[ere.index("SELECT CONDITION-VECT([C]):") + 1:ere.index("HAVING CLAUSE (G):")]
    # print(f"C: {select_conds}")
    mf_struct['C'] = select_conds
    # uncomment line above if we decide to handle mf_struct differently

    # get the having clause vector (if it exists)
    having_conds = ere[ere.index("HAVING CLAUSE (G):") + 1:]
    if len(having_conds) >= 1 and having_conds[0] != '-':
        # print(f"G: {having_conds}")
        mf_struct['G'] = having_conds
    # uncomment line above if we decide to handle mf_struct differently
    # print(mf_struct)
    # print(f"Columns of mf_struct: {list(mf_struct.keys())}")
    return mf_struct

def create_bitmaps(full_table, grouping_attributes):
 
    # Create a set to find unique values of Grouping attributes
    if len(grouping_attributes) == 1:
        unique_groups = set(row[grouping_attributes[0]] for row in full_table)
    else:
        unique_groups = set(
            tuple(row[attr] for attr in grouping_attributes) for row in full_table
        )

    # Create a bitmap for each unique value or combination of values
    bitmaps = {}

    for group_key in unique_groups:
        bitmap = []
        for row in full_table:
            if len(grouping_attributes) == 1:
                if row[grouping_attributes[0]] == group_key:
                    bitmap.append(1)
                else:
                    bitmap.append(0)
            else:
                if tuple(row[attr] for attr in grouping_attributes) == group_key:
                    bitmap.append(1)
                else:
                    bitmap.append(0)
            bitmaps[group_key] = bitmap
        else:
            bitmaps[group_key] = bitmap

    return bitmaps


def parse_gv_predicates(gv_predicates):
   
   conditions = []
   # still need to implement later
   
   return conditions

def extract_rows_bitmap(bitmap, full_table):
    rows = []

    for index, bit in enumerate(bitmap):
        if bit == 1:
            rows.append(full_table[index])  # Add the row if the bitmap value is 1

    return rows

def main_algoritm(mf_struct):
    
    generated_code = []

    for gv_predicates in mf_struct['C']:
        conditions = parse_gv_predicates(gv_predicates)
        generated_code.append(f"if {conditions}:")
        generated_code.append("    _buckets.append(row)")

    # Generate the final algorithm block
    algoritm = "\n".join(generated_code)
    return algoritm

def print_dict_as_table(array):
    print(tabulate.tabulate(array,headers="keys", tablefmt="psql"))  