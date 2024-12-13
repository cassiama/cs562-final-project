import tabulate, re
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
        # Single attribute case: wrap the single attribute value in a tuple
        unique_groups = set((row[grouping_attributes[0]],) for row in full_table)
    else:
        # Multiple attribute case: create keys as tuples of attribute values
        unique_groups = set(
            tuple(row[attr] for attr in grouping_attributes) for row in full_table
        )

    # Create a bitmap for each unique combination of values
    bitmaps = {}

    for group_key in unique_groups:
        bitmap = []
        for row in full_table:
            if len(grouping_attributes) == 1:
                # Single attribute case: compare with tuple key
                if (row[grouping_attributes[0]],) == group_key:
                    bitmap.append(1)
                else:
                    bitmap.append(0)
            else:
                # Multiple attributes case
                row_key = tuple(row[attr] for attr in grouping_attributes)
                if row_key == group_key:
                    bitmap.append(1)
                else:
                    bitmap.append(0)
        
        # Assign the bitmap to the corresponding group_key
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

def parse_condition(condition, group_key, grouping_attributes):

    # Step 1: Clean up whitespace around equality operators (optional)
    condition = re.sub(r"\s*=\s*", " == ", condition)  # uncomment if needed

    condition = condition.strip()  # Remove leading and trailing whitespace

    # Step 2: Replace any prefix before a dot (.) with 'row.'
    condition = re.sub(r'\b\w+\.', 'row.', condition)

    # Step 3: find any instances of grouping_attributes eg "if \scust\s exist take its index", then replace it with group_key[index]
    for attr in grouping_attributes:
        # Find the index of the attribute in grouping_attributes
        if attr in condition:
            attr_index = grouping_attributes.index(attr)  # Find the index of the attribute
            value = group_key[attr_index]  # Get the corresponding value from group_key

            # Replace occurrences of the attribute, ensuring it is isolated with spaces around it
            # We also handle the case where it's at the start or end of the string.
            condition = re.sub(rf"(?<=\s){attr}(?=\s)", f"'{value}'", condition)
    
    def convert_dot_notation_to_dict_key(condition):
        condition = re.sub(r'row\.(\w+)', r'row["\1"]', condition)
    
        return condition
    


    condition = convert_dot_notation_to_dict_key(condition)
    return condition
    
    
def print_dict_as_table(array):
    print(tabulate.tabulate(array,headers="keys", tablefmt="psql"))  