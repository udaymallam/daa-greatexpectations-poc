from turtle import setup
from ruamel import yaml
import os
import sys
import json
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
from great_expectations.checkpoint import SimpleCheckpoint

# 1.Get connection details to data (datasource, data_asset)
# 2.Configure and add a new data source/ use and existing datasource
# 3.Test connection
# 4.Create an expectation_suite
# 5.Prepare a batch request
# 6.Validate batch request
# 7.Add expectations to validator
# 8.Create expectations using profiling
# 9.Create a checkpoint
# 10.Run vaidations against check point
# 11.Build reports


# def datasource(datasource_name, host, username, database, schema, warehouse, role):
def configure_connection(datasource_name="default"):
    if not (check_datasource_config_exists(datasource_name)):
        configure_datasource(datasource_name)
        add_datasource_to_yml(datasource_name)

#read datasource details from config json
def get_datasource_config(datasource_name):
    filename = os.path.join(sys.path[0], '../config/config.json')
    # print(f'{filename}')
    with open(filename,'r') as f:
        data = json.load(f)
        # data2 = data[datasource_name]
        # print(f'{data2["host"]}')
        try:
            return data[datasource_name]
        except:
            print(f'Seems the datasource details for \"{datasource_name}\" not provided in config.json. Please provide values and continue')
            print("*"*110)
            exit()
        
# 1.Get connection details to data (datasource, data_asset)
def configure_datasource(datasource_name):
    datasource_config = get_datasource_config(datasource_name)
    datasource_name = datasource_name
    username = datasource_config["username"]
    password = os.getenv("password") # set the password from CLI using set password=<password> and Alt+F7 to clear cache. Password not required for SSO
    host = datasource_config["host"] # The host/account name (include region -- ex 'ABCD.us-east-1')
    database = datasource_config["database"] # The database name
    schema = datasource_config["schema"]  # The schema name
    warehouse = datasource_config["warehouse"]  # The warehouse name
    role = datasource_config["role"]  # The role name
    authenticator_url = "externalbrowser"  # A valid okta URL or 'externalbrowser' used to connect through SSO
    # print(f'Pass: {password}')

    if password is None:
        connection_string = f"snowflake://{username}@{host}/{database}/{schema}?authenticator={authenticator_url}&warehouse={warehouse}&role={role}"
    else:
        connection_string = f"snowflake://{username}:{password}@{host}/{database}/{schema}?warehouse={warehouse}&role={role}"

    datasource_config = {
        "name": datasource_name,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            # "default_inferred_data_connector_name": {
            #     "class_name": "InferredAssetSqlDataConnector",
            #     "include_schema_name": True,
            # },
        },
    }

    # print(datasource_config)
    # print("*"*50)

    return datasource_config

context = ge.get_context()
# print('context set')

# 2.Configure and add a new data source/ use and existing datasource
def add_datasource_to_yml(datasource_name):
    datasource_config = configure_datasource(datasource_name)
    try:
        context.add_datasource(**datasource_config)
        print('data source config added')
    except:
        print('Please check datasource adding to YML error')

# 3.Test connection
def test_config_connection():
    datasource_config = configure_datasource()
    try:
        context.test_yaml_config(yaml.dump(datasource_config))
        print('Data source config successful')
    except:
        print('Please check datasource config, the connection is UNSUCCESSFUL')

# print(f'{context.list_datasources()}')
# datasource_names = [datasource["name"] for datasource in context.list_datasources()]

# 4.Create an expectation_suite

def create_expectation_suite(expectation_suite_name):
    # context.create_expectation_suite(expectation_suite_name=expectation_suite_name, overwrite_existing=True)
    try:
        suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.')
    except:
        suite = context.create_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')

def check_datasource_config_exists(datasource_name):
    for datasource in context.list_datasources():
        if datasource["name"] == datasource_name:
            print(f'Datasource with name \"{datasource_name}\" configured already, will proceed with the configured details')
            print("*"*95)
            return True
    else:
        return False

def getConnectionString(datasource_name):
    for datasource in context.list_datasources():
        if datasource["name"] == datasource_name:
            return datasource["execution_engine"]["connection_string"]
    

def getDatabaseAndSchemaName(datasource_name):
    database_schema_details = getConnectionString(datasource_name).split("?")[0].split("/")[-2:]
    # print(f'----> {datasource_names}')
    return database_schema_details

# 5.Prepare a batch request
def prepare_batch_request(datasource_name, table_name, condition):
    db_schema = getDatabaseAndSchemaName(datasource_name)
    if condition is None:
        condition =""
    query =  f'SELECT * from {db_schema[0]}.{db_schema[1]}.\"{table_name}\" {condition}'
    # print(f'query= {query}')

    batch_request_details = {
        "datasource_name": datasource_name,
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": table_name,
        "runtime_parameters": {'query': f'SELECT * from {db_schema[0]}.{db_schema[1]}.\"{table_name}\" {condition}'},
        "batch_identifiers": {"default_identifier_name": "default_identifier"}
    }
    return batch_request_details

# 6.Validate batch request
def setup_expectations_validator(datasource_name, exception_suite_name, table_name, condition=None):
    configure_connection(datasource_name)
    create_expectation_suite(exception_suite_name)
    batch_request_details = prepare_batch_request(datasource_name, table_name, condition)

    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(**batch_request_details),
        expectation_suite_name= exception_suite_name
    )

    return validator

def show_validator_columns_and_head(validator):
    column_names = [f'"{column_name}"' for column_name in validator.columns()]
    print(f"Columns: {', '.join(column_names)}.")
    print(validator.head(n_rows=5, fetch_all=False))


# 8.Create expectations using profiling
def create_profiler_expectations(validator):
    profiler = UserConfigurableProfiler(
        profile_dataset=validator,
        excluded_expectations=None,
        # ignored_columns=ignored_columns,
        not_null_only=False,
        primary_or_compound_key=False,
        semantic_types_dict=None,
        table_expectations_only=False,
        value_set_threshold="MANY",
    )

    suite = profiler.build_suite()

def close_validator(validator):
    validator.execution_engine.close()

# 7.Add expectations to validator
def save_expectation_suite(validator):
    # validator = setup_expectations_validator()
    # print(validator.get_expectation_suite(discard_failed_expectations=False))
    validator.save_expectation_suite(discard_failed_expectations=False)
    # print("********************** expectation suite saved **********************")

def configure_checkpoint_old(datasource_name, table_name, expectation_suite_name, condition):
    batch_request_details = prepare_batch_request(datasource_name, table_name, condition)
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request_details,
                "expectation_suite_name": expectation_suite_name
            }
        ]
    }

    checkpoint = SimpleCheckpoint(
        f"_tmp_checkpoint_{expectation_suite_name}",
        context,
        **checkpoint_config
    )
    print(f'{checkpoint}')
    return checkpoint


# 9.Create a checkpoint
def configure_checkpoint(checkpoint_name, datasource_name, table_name, expectation_suite_name, condition=None):
    configure_connection(datasource_name)
    batch_request_details = prepare_batch_request(datasource_name, table_name, condition)
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request_details,
                "expectation_suite_name": expectation_suite_name
            }
        ]
    }

    context.add_checkpoint(**checkpoint_config)

# 10.Run vaidations against check point
def run_checkpoint(checkpoint_name):
    checkpoint_result = context.run_checkpoint(checkpoint_name = checkpoint_name)
    print(f'{checkpoint_result}')
    create_reports(checkpoint_result)

def run_checkpoint_old(datasource_name, table_name, expectation_suite_name, condition):
    checkpoint =configure_checkpoint_old(datasource_name, table_name, expectation_suite_name, condition)
    checkpoint_result = checkpoint.run()
    # print(f'{checkpoint_result}')
    # print("********************** checkpoint result created **********************")
    create_reports(checkpoint_result)
    # return checkpoint_result

# 11.Build reports
def create_reports(checkpoint_result):
    context.build_data_docs()
    validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
    context.open_data_docs(resource_identifier=validation_result_identifier)

def delete_datasource_configuration(datasource_name):
    context.delete_datasource(datasource_name)

def delete_all_datasource_configurations():
    for datasource in context.list_datasources(): 
        context.delete_datasource(datasource["name"])