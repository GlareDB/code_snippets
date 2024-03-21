"""
This is meant to be run in a notebook - that will enable an iterative 
workflow, whereby you can rapidly test things out and make changes. 
Before doing that, we recommend setting up a virtual environment,
and then you will need to install glaredb, great-expectations, and 
jupyter: 
> python3 -m venv venv
> source ./venv/bin/activate
> pip install glaredb great-expectations jupyter
"""

import glaredb
con = glaredb.connect(<YOUR CLOUD CONNECTION STRING>)

# set up a connection to Snowflake
con.execute(
    """
        CREATE EXTERNAL DATABASE snowflake_tree_db
          FROM snowflake
          OPTIONS (
            account = 'wy22406.us-central1.gcp',
            username = 'talglaredb',
            password = '<password>',
            database = 'nyc_tree_db',
            warehouse = 'COMPUTE_WH',
            role = '<role>',
          );
    """
)

# test query
con.execute(
    """
        SELECT * FROM snowflake_tree_db.public.nyc_trees LIMIT 10;
    """
)

# join across GlareDB and Snowflake and see the results
df = con.sql(
    """
        SELECT nyc_sales."BIN" bin,
                nyc_sales."ADDRESS" address,
                nyc_sales."ZIP CODE" zip_code,
                nyc_sales."SALE DATE" sale_date,
                nyc_sales."SALE PRICE" sale_price,
                COUNT (distinct tree.spc_latin) number_of_tree_species,
                COUNT (tree.spc_latin) number_of_trees
        FROM
        nyc_sales LEFT JOIN snowflake_sandbox.public.nyc_tree_data tree
        ON nyc_sales."BIN" = tree.bin
        GROUP BY nyc_sales."BIN",
                nyc_sales."ADDRESS",
                nyc_sales."ZIP CODE",
                nyc_sales."SALE DATE",
                nyc_sales."SALE PRICE"
    """).to_pandas()

df.head()

# import Great Expectations
import great_expectations as gx

# initialize Great Expectations
# context_root_dir specifies where to save your GX configuration
context = gx.get_context(context_root_dir='./')

# pass the dataframe you created above into a GX Validator object
validator = context.sources.pandas_default.read_dataframe(df)

# confirm that the data were successfully passed to GX
validator.head()

# run your first Expectation
validator.expect_column_values_to_not_be_null("address")

# run another Expectation and watch it fail
validator.expect_column_values_to_be_between(
    "number_trees",
    min_value=1,
    max_value=10
)

# update the values and watch it pass
validator.expect_column_values_to_be_between(
    "number_trees",
    min_value=0,
    max_value=1500
)

# saving your Expectation Suite
# first, give the Expectation Suite a descriptive name
validator.expectation_suite_name = "nyc_tree_suite"

# second, save the Expectation Suite
validator.save_expectation_suite()

# add a Checkpoint to run your Expectation Suite
checkpoint = context.add_or_update_checkpoint(
    name="tree_suite_checkpoint",
    validator=validator,
)

# run the Checkpoint
checkpoint_result = checkpoint.run()

# view the results
context.view_validation_result(checkpoint_result)

# now, rerun everything above, but with new data;
# this will join Parquet files to Snowflake data
df = con.sql(
    """
        SELECT nyc_sales."BIN" bin,
                nyc_sales."ADDRESS" address,
                nyc_sales."ZIP CODE" zip_code,
                nyc_sales."SALE DATE" sale_date,
                nyc_sales."SALE PRICE" sale_price,
                COUNT (distinct tree.spc_latin) number_tree_species,
                COUNT (tree.spc_latin) number_trees
        FROM
        '../nyc_sales/SALE_YEAR_2020/**/*.parquet' nyc_sales
        LEFT JOIN snowflake_sandbox.public.nyc_tree_data tree
        ON nyc_sales."BIN" = tree.bin
        GROUP BY nyc_sales."BIN",
                nyc_sales."ADDRESS",
                nyc_sales."ZIP CODE",
                nyc_sales."SALE DATE",
                nyc_sales."SALE PRICE"
    """).to_pandas()

df.head()

# re-initialize and test your validator
validator = context.sources.pandas_default.read_dataframe(df)

validator.head()

# get and run the checkpoint
checkpoint = context.get_checkpoint("tree_suite_checkpoint")

checkpoint_result = checkpoint.run()

# view the results; this time the Expectations failed
context.view_validation_result(checkpoint_result)

# despite that, you think the data are right and the Expectations
# were wrong, so you'll load the data into your GlareDB table anyway
con.execute(
    """
        INSERT INTO nyc_sales
        SELECT * FROM '../nyc_parquet/SALE_YEAR_2020/**/*.parquet'
    """
)

# and then update the Expectation to match your new understanding
validator.expect_column_values_to_be_between(
    "number_trees",
    min_value=0,
    max_value=1700)

# and save the Expectation Suite
validator.save_expectation_suite()

# 🎉
