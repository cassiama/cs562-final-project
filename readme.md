# CS 526 Final Project

## Intro

The purpose of this project is to generate code that processes E-SQL queries. Our code is not a query processor in and of itself. The main contribution that we provided to this project was the algorithm which is used by the query processor.

## Caveats

- This code was not developed with the goal of creating optimized code. Its purpose is to convert the arguments for the Φ (phi) operator into the algorithmic steps required to process it.
- We had to come up with our own E-SQL relational expressions for this project. Most of them are based on examples from class.

## How to Run

- Step 0: Download and set up PostgreSQL Server on your machine. Make sure to initialize your credentials and put them in your own .env file.
- Step 1: Install all of the dependencies by running

```py
pip install -r requirements.txt
```

- Step 2: Run the `generator.py` file and view the results in `_generated.py`.

- Step 3: Type '1' to manually input the arguments for the Φ (phi) operator or type '2' to run one of the query text files.
