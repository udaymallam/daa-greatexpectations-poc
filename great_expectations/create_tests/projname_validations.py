import test_helper
# configure check points
test_helper.configure_checkpoint("poc_table_ckpoint", "snowflake5","POC_TABLE","Poc_suite")
test_helper.configure_checkpoint("poc_table2_ckpoint", "snowflake1","POC_TABLE2","Poc_suite")

#Run check points
test_helper.run_checkpoint("poc_table_ckpoint")
test_helper.run_checkpoint("poc_table2_ckpoint")
