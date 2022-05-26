import test_helper

#Setup Expectations Validator
validator = test_helper.setup_expectations_validator("snowflake", "Poc_suite", "POC_TABLE")

# View table head
# test_helper.show_validator_columns_and_head(validator)

#Create  expectations using profiler
test_helper.create_profiler_expectations(validator)

#Create expectations manually
validator.expect_column_values_to_not_be_null("bid")
validator.expect_column_values_to_be_unique("spid")

#Save expectations
test_helper.save_expectation_suite(validator)