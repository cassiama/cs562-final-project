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


def main_algoritm(input_file_no):
    mf_struct = mf_struct_from_input_file(input_file_no)
    print(mf_struct)
    # go through each such that clause
    for i in mf_struct['C']:
        print(i)
     