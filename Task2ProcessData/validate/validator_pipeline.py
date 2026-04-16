# Author: 

from pyspark.sql import functions as F
from validate.validity_rules import ValidityRules
from validate.error_labeler import ErrorLabeler

class ValidatorPipeline:
    @staticmethod
    def run(df):
        validated = ValidityRules.apply(df)
        valid = validated.filter(F.col("is_valid")).drop("is_valid")
        invalid = validated.filter(~F.col("is_valid"))
        invalid_labeled = ErrorLabeler.apply(invalid)
        return valid, invalid, invalid_labeled
