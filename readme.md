# CS 526 Final Project

## Intro

The purpose of this project is to generate code that processes E-SQL queries. Our code is not a query processor in and of itself. The main contribution that we provided to this project was the algorithm which is used by the query processor.

## Caveats

- This code was not developed with the goal of creating optimized code. Its purpose is to convert the arguments for the Φ (phi) operator into the algorithmic steps required to process it.
- We had to come up with our own E-SQL relational expressions for this project. Most of them are based on examples from class.

## How to Run

- Step 0: Download and set up PostgreSQL Server on your machine. Make sure to initialize your credentials and put them in your own .env file.
- Step 1: Before continuing, we recommend creating a virtual environment first (`python -m venv {your_venv}`).
- Step 2: Next, activate the virtual environment. It's different for each major OS.
  - Windows (PowerShell): `{your_venv}\Scripts\activate.ps1`
  - Windows (Command Prompt): `{your_venv}\Scripts\activate.bat`
  - Linux/MacOS: `source {your_env}/bin/activate`
- Step 3: Then, install all of the dependencies by running

```py
pip install -r requirements.txt
```

- Step 4: Run the following line:
```py
{your_venv}/Scripts/python generator.py
```

This *ensures* that it will not give you the following message: `ModuleNotFoundError: No module named 'psycopg2'`. Running it with just `python` **might** work, but it is inconsistent. If you are interested in learning why, [read this article](https://docs.python.org/3/library/venv.html) to get an idea.

Anyway, after running that command in the terminal, `_generated.py` will immediately start running.

- Step 5: Type '1' to manually input the arguments for the Φ (phi) operator or type '2' to run one of the query text files.

Once it has finished running, it will show you the results of your intended E-SQL query.

## Limitations
Running the `test_generator.py` file won't work due to the menus in the `generator.py` and `sql.py` files. In order to verify that the E-SQL queries match the SQL queries, you will have to run and compare the results of each file manually.
