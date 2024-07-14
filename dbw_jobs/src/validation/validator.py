import yaml
from pyspark.sql.functions import col, expr, row_number, struct, lit, udf, to_json
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType
import re

class DataValidator:
    def __init__(self, rules_file):
        with open(rules_file, 'r') as file:
            self.config = yaml.safe_load(file)
        self.rules = self.config['rules']
        self.actions = self.config['actions']

    def validate(self, df):
        invalid_data = None
        validation_errors = {}
        for rule in self.rules:
            df, invalid, error = self._apply_rule(df, rule)
            if invalid is not None:
                invalid_data = invalid if invalid_data is None else invalid_data.union(invalid)
            if error:
                validation_errors[rule['name']] = error

        if invalid_data is not None:
            invalid_data = invalid_data.withColumn("validation_errors", struct([lit(v).alias(k) for k, v in validation_errors.items()]))
            invalid_data = invalid_data.withColumn('validation_errors', to_json(col('validation_errors')))

        return df, invalid_data

    def _apply_rule(self, df, rule):
        check_type = rule['check']
        invalid_data = None
        error = None

        if check_type == 'not_null':
            invalid_data = df.filter(col(rule['field']).isNull())
        elif check_type == 'range':
            invalid_data = df.filter((col(rule['field']) < rule['min']) | (col(rule['field']) > rule['max']))
        elif check_type == 'custom':
            invalid_data = df.filter(~expr(rule['condition']))
        elif check_type == 'unique':
            window = Window.partitionBy(rule['fields']).orderBy(col("lastUpdated").desc())
            df = df.withColumn("row_num", row_number().over(window))
            invalid_data = df.filter(col("row_num") > 1).drop("row_num")
            df = df.filter(col("row_num") == 1).drop("row_num")
        elif check_type == 'regex':
            regex_check = udf(lambda x: bool(re.match(rule['pattern'], x)) if x else False, BooleanType())
            invalid_data = df.filter(~regex_check(col(rule['field'])))
        else:
            raise ValueError(f"Unknown check type: {check_type}")

        if invalid_data and invalid_data.count() > 0:
            error = f"Failed {rule['name']}"
            df = df.exceptAll(invalid_data)

        return df, invalid_data, error

    def _take_action(self, df, result):
        action = result['action']
        if action == 'fail' and not result['passed']:
            raise ValueError(f"Validation failed: {result['rule']}")
        elif action == 'log':
            print(f"Validation {'passed' if result['passed'] else 'failed'}: {result['rule']}")
        
        return df

    def _deduplicate_data(self, df, rule):
        fields = rule.get('fields', df.columns)
        window_spec = Window.partitionBy(*fields).orderBy(col("lastUpdated").desc())
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
        deduplicated_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
        return deduplicated_df