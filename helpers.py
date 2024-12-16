import tabulate, re
from collections import defaultdict
import datetime

# global variables (used for validation & parsing functions)
sales_columns = ['cust', 'prod', 'day', 'month', 'year', 'state', 'quant', 'date']
sales_columns_types = {
    'int': ['day', 'month', 'year', 'quant'],
    'datetime': ['date'],
    'str': ['cust', 'prod', 'state']
}
sql_aggregates = ['avg', 'count', 'min', 'max', 'sum']
sql_cond_ops = ['and', 'or', 'not']
sql_comparison_ops = ['=', '>', '<', '>=', '<=', '!=']

'''
Input:
arg - the attribute to be validated. must be a valid attribute in the sales table.

Returns true if 'arg' is a valid column in the sales table.
'''
def validate_attribute(arg: str) -> bool:
    return arg in sales_columns

'''
Input:
arg - the aggregate to be validated. must be an aggregate on a valid attribute.

Returns true if the 'arg' is a valid aggregate on a valid attribute.
'''
def validate_aggregate(arg: str) -> bool:
    return any([arg.startswith(agg) for agg in sql_aggregates]) and any([arg.endswith(col) for col in sales_columns])

'''
Input:
arg - the group number to be validated. must be a number > 0 and < the maximum number of possible grouping variables.
max_group_number - the maximum number of grouping variables

Returns true if the 'arg' is a valid group number.
'''
def validate_group(arg: str, max_group_number: int) -> bool:
    try:
        arg = int(arg)
        if arg < 0 or arg > max_group_number:
            return False
    except ValueError:
        return False
    return True

'''
Input:
arg - the condition to be validated. must be a valid condition for WHERE, SUCH THAT, or HAVING clause.
max_group_number - the maximum number of grouping variables
possible_columns - list of all columns that have been parsed from user input
grouping_cond - boolean that determines if the condition contains grouping variables
such_that - boolean that determines whether the condition is for the such that clause or having clause

Returns true if the 'arg'
'''
def validate_condition(
    arg: str,
    max_group_number: int,
    possible_columns: list = [],
    grouping_cond: bool = True,
    such_that: bool = True
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
            if such_that:
                if '_' in lefthand or '_' in righthand:
                    valid_lefthand = valid_righthand = False
                else:
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
            else:
                if '.' in lefthand or '.' in righthand:
                    valid_lefthand = valid_righthand = False
                else:
                    _, group, col = lefthand.split('_')
                    valid_lefthand = validate_aggregate(lefthand) and validate_group(group, max_group_number) and validate_attribute(col)

                    _, group, col = righthand.split('_')
                    valid_righthand = validate_aggregate(righthand) and validate_group(group, max_group_number) and validate_attribute(col)

        valid_op = operator in sql_comparison_ops
        
        valid_conditions.append(valid_lefthand and valid_op and valid_righthand)
    return all(valid_conditions)

'''
Input: None

Returns a 'mf_struct' dictionary from user input.
'''
def mf_struct_from_user_input():
        mf_struct = {}
        n_gv = 0
        select_attr = []
        group_attr = []
        where_clause = []
        aggregates = []
        select_conds = []
        having_conds = []

        args_parsed = 0
        max_args = 7
        invalid_args = []
        possible_columns = []
        input_notes = ''
        while args_parsed != max_args:
            if args_parsed == 0:
                input_notes = 'What attributes & aggregates do you want to select? Format: attribute, aggregate_1_attribute, aggregate_2_attribute...\n'

                # get the select attributes
                select_attr = input("SELECT ATTRIBUTE(S):\n")

                # if empty, start from scratch
                if not select_attr:
                    print('Input is empty. Try again.')
                    print(input_notes)
                    invalid_args.clear()
                    continue

                select_attr = select_attr.split(', ')
                # check if it's a valid sales column
                valid_column = validate_attribute(select_attr[0])
                if len(select_attr) == 1:
                    if not valid_column:
                        print(f"Invalid argument parsed: {select_attr[0]}. Try again.")
                        print(input_notes)
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
                        print(f"Invalid argument parsed: {', '.join(invalid_args)}. Try again.")
                        print(input_notes)
                        invalid_args.clear()
                        continue

                for attr in select_attr:
                    mf_struct[attr] = []
                mf_struct['S'] = select_attr
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 1:
                # get the number of grouping variables
                input_notes = 'How many grouping variables?\n'
                try:
                    input_n = input("NUMBER OF GROUPING VARIABLES(n):\n")
                    n_gv = int(input_n)
                    if n_gv < 0:
                        print(f"Invalid argument parsed: {n_gv}. Try again")
                        print(input_notes)
                        invalid_args.clear()
                        continue

                    mf_struct['n'] = n_gv
                    args_parsed += 1
                    invalid_args.clear()
                except ValueError:
                    print(f"Invalid argument parsed: {input_n}. Try again")
                    print(input_notes)
                    invalid_args.clear()
                    continue

            if args_parsed == 2:
                input_notes = 'What attributes do you want to group by? Format: attribute1, attribute2, ...\n'
                group_attr = input("GROUPING ATTRIBUTES(V):\n")

                # if empty, start from scratch
                if not group_attr:
                    print('Input is empty. Try again.')
                    print(input_notes)
                    invalid_args.clear()
                    continue

                group_attr = group_attr.split(', ')
                # check if it's a valid sales column
                valid_column = validate_attribute(group_attr[0])
                if len(group_attr) == 1 and not valid_column:
                    print(f"Invalid argument parsed: {group_attr[0]}. Try again.")
                    print(input_notes)
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
                        print(f"Invalid argument parsed: {', '.join(invalid_args)}. Try again.")
                        print(input_notes)
                        invalid_args.clear()
                        continue

                mf_struct['V'] = group_attr
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 3:
                input_notes = 'What\'s your WHERE clause? Format: attribute comparison_op "val"\nPress Enter to continue.\n'
                where_clause = input("WHERE CLAUSE(W):\n")
                
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
                    print(input_notes)
                    invalid_args.clear()
                    continue

                mf_struct['W'] = where_clause
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 4:
                input_notes = "What aggregates are included in your SELECT clause? NOTE: Reuse the aggregates you used for the SELECT step.\n"
                # get the aggregates vector
                aggregates = input("F-VECT([F]):\n")
                # if empty, start from scratch
                if not aggregates or aggregates == '-':
                    mf_struct['F'] = ['-']
                    invalid_args.clear()
                    args_parsed += 1
                    continue

                aggregates = aggregates.split(', ')
                for agg in aggregates:
                    # if aggregate not already been parsed in the SELECT clause, prompt to try again
                    valid_aggregate = validate_aggregate(agg)
                    if agg not in mf_struct['S'] or not valid_aggregate:
                        invalid_args.append(agg)
                
                if len(invalid_args) > 0:
                    print(f"Invalid argument parsed: {', '.join(invalid_args)}. All aggregates you input must be present in the SELECT clause. Try again.\n")
                    print(input_notes)
                    invalid_args.clear()
                    continue

                mf_struct['F'] = aggregates
                args_parsed += 1
                invalid_args.clear()

            # get the select condition vector
            if args_parsed == 5:
                input_notes = f'What\'s your SUCH THAT clause? Format: group_number.attribute comparison_op "val"\nPress Enter to continue.\n'
                such_that_clause = input("SELECT CONDITION-VECT([C]):\n")
                
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
                    print(input_notes)
                    invalid_args.clear()
                    continue

                select_conds.append(such_that_clause)
                while such_that_clause:
                    such_that_clause = input("What other conditions would you like to include?\nPress Enter to continue.\n")
                    if not such_that_clause:
                        break

                    # make sure the such that clause was valid
                    valid_condition = validate_condition(such_that_clause, mf_struct['n'], possible_columns, grouping_cond=True)
                    if not valid_condition:
                        invalid_args.append(such_that_clause)
                        break

                    # if such that clause is valid, add to 'C' in mf_struct
                    select_conds.append(such_that_clause)
                
                if len(invalid_args) > 0:
                    print(f"Invalid argument parsed: {', '.join(invalid_args)}")
                    print(input_notes)
                    invalid_args.clear()
                    continue

                mf_struct['C'] = select_conds
                args_parsed += 1
                invalid_args.clear()

            if args_parsed == 6:
                # get the having clause vector (if it exists)
                input_notes = f'What\'s your HAVING clause? Format: aggregate_1_attribute comparison_op aggregate_2_attribute\nPress Enter to continue.\n'
                having_clause = input("HAVING CLAUSE (G):\n")                
                
                # if empty, then move on to the next argument
                if not having_clause:
                    args_parsed += 1
                    invalid_args.clear()
                    continue

                # make sure the having clause was valid
                valid_condition = validate_condition(having_clause, mf_struct['n'], possible_columns, grouping_cond=True, such_that=False)
                if not valid_condition:
                    print(f"Invalid argument parsed: {having_clause}. Please try again.")
                    print(input_notes)
                    invalid_args.clear()
                    continue

                having_conds.append(having_clause)
                mf_struct['G'] = having_conds
                args_parsed += 1
                invalid_args.clear()

        return mf_struct

'''
Input:
input_file_no - the query number

Returns a 'mf_struct' dictionary from given input file.
'''
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
    for attr in select_attr:
        mf_struct[attr] = []
    mf_struct['S'] = select_attr

    # get the number of grouping variables
    n_gv = int(ere[ere.index("NUMBER OF GROUPING VARIABLES(n):") + 1])
    mf_struct['n'] = n_gv

    # get the grouping attributes
    group_attr = ere[ere.index("GROUPING ATTRIBUTES(V):") + 1].split(', ')
    mf_struct['V'] = group_attr

    # get the where clause
    if "WHERE CLAUSE(W):" in ere:
        where_clause = ere[ere.index("WHERE CLAUSE(W):") + 1]
        mf_struct['W'] = where_clause if where_clause else '-'
    else:
        mf_struct['W'] = '-'

    # get the aggregates vector
    aggregates = ere[ere.index("F-VECT([F]):") + 1].split(', ')
    mf_struct['F'] = aggregates

    # get the select condition vector
    select_conds = ere[ere.index("SELECT CONDITION-VECT([C]):") + 1:ere.index("HAVING CLAUSE (G):")]
    mf_struct['C'] = select_conds
    
    # get the having clause vector (if it exists)
    having_conds = ere[ere.index("HAVING CLAUSE (G):") + 1:]
    if len(having_conds) >= 1 and having_conds[0] != '-':
        mf_struct['G'] = having_conds

    return mf_struct

'''
Input:
full_table - the full sales table
grouping_attributes - the group key (attributes from sales used to group the results)

Returns a dictionary where:
    - key - a tuple of grouping attributes
    - value - a "bitmap" (list of 1s and 0s) which represents rows belonging to the group key.
        - 1 means row is in table, 0 means row isn't in table
'''
def create_bitmaps(full_table, grouping_attributes):
    if len(grouping_attributes) == 1:
        unique_groups = set((row[grouping_attributes[0]],) for row in full_table)
    else:
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
        
        bitmaps[group_key] = bitmap

    return bitmaps

'''
Input:
bitmap - a list of 1s and 0s which represent rows in the table
full_table - the full sales table

Returns a list of dictionaries where each dictionary has a:
    - key - attribute in the sales table
    - value - value for attribute in that row
'''
def extract_rows_bitmap(bitmap, full_table):
    rows = []

    for index, bit in enumerate(bitmap):
        if bit == 1:
            rows.append(full_table[index])  

    return rows

'''
Input:
condition - the condition to be parsed. must be a condition from the user input or an input file.
group_key - the attributes from sales used to group the results
grouping_attributes - the grouping attributes (used in phi operator). must be from the user input or an input file.

Returns the parsed condition (transformed for evaluation).
'''
def parse_condition(condition: str, group_key, grouping_attributes):

    condition = condition.replace(" = ", " == ")
    condition = re.sub(r'\b\w+\.', 'row.', condition)

    for attr in grouping_attributes:

        if attr in condition:
            attr_index = grouping_attributes.index(attr)  
            value = group_key[attr_index]  

            if attr in sales_columns_types['str']:
                condition = re.sub(rf"(?<=\s){attr}(?=\s)", f"'{value}'", condition)
            elif attr in sales_columns_types['int']:
                condition = re.sub(rf"(?<=\s){attr}(?=\s)", f"{value}", condition)
            else:
                year, month, day = value.split("-")
                condition = re.sub(rf"(?<=\s){attr}(?=\s)", f"{datetime.datetime(year, month, day)}", condition)
    
    def convert_dot_notation_to_dict_key(condition):
        condition = re.sub(r'row\.(\w+)', r'row["\1"]', condition)
        return condition
    
    condition = convert_dot_notation_to_dict_key(condition)
    return condition

'''
Input:
condition - the condition to be parsed. must be a WHERE clause condition from the user input or an input file.

Returns the parsed condition (transformed for evaluation) for the WHERE clause.
'''
def parse_where_condition(condition):

    condition = condition.replace(" = ", " == ")
    all_columns = sum(sales_columns_types.values(), [])

    def prefix_with_row(match):
        column_name = match.group(1)
        return f'row["{column_name}"]'

    column_pattern = r'\b(' + '|'.join(re.escape(col) for col in all_columns) + r')\b'
    condition = re.sub(column_pattern, prefix_with_row, condition)

    def convert_datetime_literals(match):
        literal = match.group(1)
        try:  
            datetime_obj = datetime.datetime.strptime(literal, "%Y-%m-%d")   
            return f'"{datetime_obj.date()}"'
        except ValueError:
            return f"'{literal}'"

    condition = re.sub(r"'(\d{4}-\d{2}-\d{2})'", convert_datetime_literals, condition)
    return condition

'''
Input:
condition - the condition to be parsed. must be a HAVING clause condition from the user input or an input file.

Returns the parsed condition (transformed for evaluation) for the HAVING clause.
'''
def parse_having_condition(condition):
    comparison_operators = r"(<=|>=|<|>|==|!=)"

    def add_row_prefix(match):
        identifier = match.group(1)
        return f'row["{identifier}"]'


    def transform_condition(condition):
        parts = re.split(comparison_operators, condition)
        transformed = []
        for i, part in enumerate(parts):
            part = part.strip()
            if i % 2 == 0: 
                part = re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', add_row_prefix, part)
            transformed.append(part)
        return " ".join(transformed)
    
    condition = transform_condition(condition)
    return condition

'''
Input:
dict - a dictionary where:
    - key - an attribute in the sales table or aggregate on an attribute
    - value - a list of all values for given attribute

Prints out the dictionary as a SQL table.
'''
def print_dict_as_table(dict):
    print(tabulate.tabulate(dict,headers="keys", tablefmt="psql"))  