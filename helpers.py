import tabulate, re
from collections import defaultdict

sales_columns = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date']
sql_aggregates = ['avg', 'count', 'min', 'max', 'sum']
sql_cond_ops = ['and', 'or', 'not']
sql_comparison_ops = ['=', '>', '<', '>=', '<=', '<>']

def validate_attribute(arg: str) -> bool:
    return arg in sales_columns

def validate_aggregate(arg: str) -> bool:
    return any([arg.startswith(agg) for agg in sql_aggregates]) and any([arg.endswith(col) for col in sales_columns])

def validate_group(arg: str, max_group_number: int) -> bool:
    try:
        arg = int(arg)
        if arg < 0 or arg > max_group_number:
            return False
    except ValueError:
        return False
    return True

def validate_condition(
    arg: str,
    max_group_number: int,
    possible_columns: list = [],
    grouping_cond: bool = True
) -> bool:
    valid_conditions = []
    valid_lefthand = valid_op = valid_righthand = False
    args = arg.split(' ')
    # if we find more 1 condition, check if there are at least 1+ condition operators; if not, return False
    if len(args) > 3 and not any([op in args for op in sql_cond_ops]):
        return False
    
    conditions = []
    if 'and' not in arg and 'or' not in arg:
        conditions.append(arg)
    else:
        if 'and' in arg:
            conditions.extend(arg.split(' and ')) # split up the conditions if there are 1+ 'and' operators 
        if 'or' in arg:
            conditions.extend(arg.split(' or ')) # split up the conditions if there are 1+ 'or' operators

        # if there are 'and' and 'or' operators, we need to remove the unsplit conditions
        temp = [cond for cond in conditions if ' and ' not in cond and ' or ' not in cond]
        conditions = temp.copy()
        
    for cond in conditions:
        lefthand, operator, righthand = cond.split(' ')
        # if where clause, there will be no "groups" in the condition
        if not grouping_cond:
            valid_lefthand = lefthand in sales_columns
            if righthand.count("'") == 0:
                try:
                    righthand = int(righthand)
                    valid_righthand = True
                except ValueError:
                    valid_righthand = righthand in sales_columns and righthand in possible_columns
            else:
                valid_righthand = True
        # if not, then there will be a group in the condition
        else:
            # for such that clauses
            if '.' in lefthand:
                group, col = lefthand.split('.')
                valid_lefthand = validate_group(group, max_group_number) and validate_attribute(col)
                if righthand.count("'") == 0:
                    try:
                        righthand = int(righthand)
                        valid_righthand = True
                    except ValueError:
                        valid_righthand = righthand in sales_columns and righthand in possible_columns
                else:
                    valid_righthand = True

            # for having clauses
            elif '_' in lefthand:
                _, group, col = lefthand.split('_')
                valid_lefthand = validate_aggregate(lefthand) and validate_group(group, max_group_number) and validate_attribute(col)

                _, group, col = righthand.split('_')
                valid_righthand = validate_aggregate(righthand) and validate_group(group, max_group_number) and validate_attribute(col)

        valid_op = operator in sql_comparison_ops
        
        valid_conditions.append(valid_lefthand and valid_op and valid_righthand)
    return all(valid_conditions)


def mf_struct_from_user_input():
        mf_struct = {}
        n_gv = 0
        select_attr = group_attr = where_conds = aggregates = select_conds = having_conds = []

        args_parsed = 0
        max_args = 7
        invalid_args = []
        possible_columns = []
        while args_parsed != max_args:
            if args_parsed == 0:      
                # get the select attributes
                select_attr = input('What attributes & aggregates do you want to select? Format: attribute, aggregate_1_attribute, aggregate_2_attribute...\n')
                # if empty, start from scratch
                if not select_attr:
                    print('Input is empty. Try again.')
                    invalid_args.clear()
                    continue

                select_attr = select_attr.split(', ')
                # check if it's a valid sales column
                valid_column = validate_attribute(select_attr[0])
                if len(select_attr) == 1:
                    if not valid_column:
                        print(f'Invalid argument parsed: {select_attr[0]}. Try again.')
                        invalid_args.clear()
                        continue
                    possible_columns.append(select_attr[0])

                else:
                    for attr in select_attr:
                        # check if it's a valid sales column or an aggregate first
                        valid_column = validate_attribute(attr)
                        valid_aggregate = validate_aggregate(attr)
                        if not valid_column and not valid_aggregate:
                            # if not, then add it to the invalid arguments
                            invalid_args.append(attr)
                        elif valid_column:
                            possible_columns.append(attr)
                            
                    if len(invalid_args) > 0:
                        print(f'Invalid argument parsed: {', '.join(invalid_args)}. Try again.')
                        invalid_args.clear()
                        continue

                for attr in select_attr:
                    mf_struct[attr] = []
                mf_struct['S'] = select_attr
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 1:
                # get the number of grouping variables
                try:
                    n_gv = int(input('How many grouping variables?\n'))
                    if n_gv < 0:
                        print(f'Invalid argument parsed: {n_gv}. Try again')
                        invalid_args.clear()
                        continue

                    mf_struct['n'] = n_gv
                    args_parsed += 1
                    invalid_args.clear()
                except ValueError:
                    print(f'Invalid argument parsed: {n_gv}. Try again')
                    invalid_args.clear()
                    continue

            if args_parsed == 2:
                group_attr = input('What attributes do you want to group by? Format: attribute1, attribute2, ...\n')

                # if empty, start from scratch
                if not group_attr:
                    print('Input is empty. Try again.')
                    invalid_args.clear()
                    continue

                group_attr = group_attr.split(', ')
                # check if it's a valid sales column
                valid_column = validate_attribute(group_attr[0])
                if len(group_attr) == 1 and not valid_column:
                    print(f'Invalid argument parsed: {group_attr[0]}. Try again.')
                    invalid_args.clear()
                    continue

                else:
                    for attr in group_attr:
                        # check if it's a valid sales column or an aggregate first
                        valid_column = validate_attribute(attr)
                        valid_aggregate = validate_aggregate(attr)
                        if not valid_column and not valid_aggregate:
                            # if not, then add it to the invalid arguments
                            invalid_args.append(attr)
                            
                    if len(invalid_args) > 0:
                        print(f'Invalid argument parsed: {', '.join(invalid_args)}. Try again.')
                        invalid_args.clear()
                        continue

                mf_struct['V'] = group_attr
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 3:
                where_clause = input(f'What\'s your WHERE clause? Format: attribute comparison_op "val"\nPress Enter to continue.\n')
                
                # if empty, then move on to the next argument
                if not where_clause:
                    mf_struct['W'] = '-'
                    args_parsed += 1
                    invalid_args.clear()
                    continue

                # make sure the where clause was valid
                valid_condition = validate_condition(where_clause, mf_struct['n'], possible_columns, grouping_cond=False)
                if not valid_condition:
                    print(f"Invalid argument parsed: {where_clause}. Please try again.")
                    invalid_args.clear()
                    continue

                mf_struct['W'] = where_clause
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 4:
                # get the aggregates vector
                aggregates = input("What aggregates are included in your SELECT clause? NOTE: Reuse the aggregates you used for the SELECT step.\n")
                # if empty, start from scratch
                if not aggregates:
                    print('Input is empty. Try again.')
                    invalid_args.clear()
                    continue

                aggregates = aggregates.split(', ')
                for agg in aggregates:
                    # if aggregate not already been parsed in the SELECT clause, prompt to try again
                    valid_aggregate = validate_aggregate(agg)
                    if agg not in mf_struct['S'] or not valid_aggregate:
                        invalid_args.append(agg)
                
                if len(invalid_args) > 0:
                    print(f'Invalid argument parsed: {', '.join(invalid_args)}. All aggregates you input must be present in the SELECT clause. Try again.\n')
                    invalid_args.clear()
                    continue

                mf_struct['F'] = aggregates
                args_parsed += 1
                invalid_args.clear()

            # get the select condition vector
            if args_parsed == 5:
                such_that_clause = input(f'What\'s your SUCH THAT clause? Format: group_number.attribute comparison_op "val"\nPress Enter to continue.\n')
                
                # if empty, then move on to the next argument
                if not such_that_clause:
                    mf_struct['C'] = '-'
                    args_parsed += 1
                    invalid_args.clear()
                    continue

                # make sure the such that clause was valid
                valid_condition = validate_condition(such_that_clause, mf_struct['n'], possible_columns)
                if not valid_condition:
                    print(f"Invalid argument parsed: {such_that_clause}. Please try again.")
                    invalid_args.clear()
                    continue

                select_conds.append(such_that_clause)
                while such_that_clause:
                    such_that_clause = input("What other conditions would you like to include?\nPress Enter to continue.\n")
                    if not such_that_clause:
                        break

                    # make sure the such that clause was valid
                    valid_condition = validate_condition(such_that_clause, mf_struct['n'], possible_columns)
                    if not valid_condition:
                        invalid_args.append(such_that_clause)
                        break

                    # if such that clause is valid, add to 'C' in mf_struct
                    select_conds.append(such_that_clause)
                
                if len(invalid_args) > 0:
                    print(f'Invalid argument parsed: {', '.join(invalid_args)}')
                    invalid_args.clear()
                    continue

                mf_struct['C'] = select_conds
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 6:
                # get the having clause vector (if it exists)
                having_clause = input(f'What\'s your HAVING clause? Format: aggregate_1_attribute comparison_op aggregate_2_attribute\nPress Enter to continue.\n')
                
                # if empty, then move on to the next argument
                if not having_clause:
                    args_parsed += 1
                    invalid_args.clear()
                    continue

                # make sure the having clause was valid
                valid_condition = validate_condition(having_clause, mf_struct['n'], possible_columns)
                if not valid_condition:
                    print(f"Invalid argument parsed: {having_clause}. Please try again.")
                    invalid_args.clear()
                    continue

                having_conds.append(having_clause)
                while having_clause:
                    having_clause = input("What other conditions would you like to include?\nPress Enter to continue.\n")
                    if not having_clause:
                        break

                    # make sure the having clause was valid
                    valid_condition = validate_condition(having_clause, mf_struct['n'], possible_columns)
                    if not valid_condition:
                        invalid_args.append(having_clause)
                        break

                    # if having clause is valid, add to 'G' in mf_struct
                    having_conds.append(having_clause)
                
                if len(invalid_args) > 0:
                    print(f'Invalid argument parsed: {', '.join(invalid_args)}')
                    invalid_args.clear()
                    continue

                mf_struct['G'] = having_conds
                args_parsed += 1
                invalid_args.clear()

        return mf_struct

def mf_struct_from_input_file(input_file_no):

    filepath = f'./q{input_file_no}.txt'
    ere = []
    with open(filepath, "r") as f:
        ere = f.read().splitlines()

    mf_struct = {}
    # print(ere)
    n_gv = 0
    select_attr = group_attr = aggregates = select_conds = having_conds = where_clause =  []
    
   
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

    # get the where clause
    if "WHERE CLAUSE(W):" in ere:
        where_clause = ere[ere.index("WHERE CLAUSE(W):") + 1]
        mf_struct['W'] = where_clause if where_clause else '-'
    else:
        mf_struct['W'] = '-'

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


def extract_rows_bitmap(bitmap, full_table):
    rows = []

    for index, bit in enumerate(bitmap):
        if bit == 1:
            rows.append(full_table[index])  # Add the row if the bitmap value is 1

    return rows

# bug : 'date' datatype , 'int' datatypes dont aggregate  , make separate parsing conditions for string, date, and int, change parse_condition, parse_where_condition, parse_having_condition , and test all input files

def parse_condition(condition: str, group_key, grouping_attributes):

    # Step 1: Replace " = " with " == " (must include spaces around equal sign)
    condition = condition.replace(" = ", " == ")

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
    
def parse_where_condition(condition):
    # Step 1: Replace " = " with " == " (must include spaces around equal sign)
    condition = condition.replace(" = ", " == ")
    sales_columns = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date']

    # Step 2: Prefix column names in sales_columns with 'row.' if they exist in the string
    for col in sales_columns:
        # Use regex to ensure matching isolated column names
        condition = re.sub(rf"\b{col}\b", f"row.{col}", condition)

    # Step 4: Convert dot notation (e.g., row.col) to dictionary key notation (row["col"])
    condition = re.sub(r"row\.(\w+)", r'row["\1"]', condition)

    return condition    


    


    condition = convert_dot_notation_to_dict_key(condition)
    return condition
    
def parse_having_condition(condition):
    # Step 1: Replace " = " with " == " for equality
    condition = condition.replace(" = ", " == ")
    
    # Step 2: Add row prefixes to identifiers but skip logical operators and keywords
    keywords = {"and", "or"}
    condition = re.sub(
        r'\b(\w+)\b',  # Match words (identifiers or keywords)
        lambda match: f'row["{match.group(1)}"]' if match.group(1) not in keywords else match.group(1),
        condition
    )
    
    return condition
    
def print_dict_as_table(dict):
    print(tabulate.tabulate(dict,headers="keys", tablefmt="psql"))  