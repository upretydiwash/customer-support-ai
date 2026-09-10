import dlt
from pyspark.sql import functions as F

class DataQualityPipeline:
    def __init__(self, source_table, quarantine_table, rules: dict):
        self.source_table = source_table
        self.quarantine_table = quarantine_table
        self.rules = rules  # e.g., {"valid_id": "customer_id IS NOT NULL"}

    def build(self):
        # Extract variables into local scope for closure evaluation
        source = self.source_table
        quarantine_target = self.quarantine_table
        rules_dict = self.rules

        # -------------------------------------------------------------
        # 1. CLEAN TABLE: Drops bad records using expectations
        # -------------------------------------------------------------
        #@dlt.table(name=clean_target)
        @dlt.expect_all_or_drop(rules_dict)
        def process_clean():
            return dlt.read(source)

        # -------------------------------------------------------------
        # 2. QUARANTINE TABLE: Filters for rows that fail ANY rule
        # -------------------------------------------------------------
        @dlt.table(name=quarantine_target)
        def process_quarantine():
            df = dlt.read(source)

            #Mapping each row to a case statement: if rule evaluated to FALSE/NULL, return rule_name
            rule_checks = [ 
                F.when(~F.expr(cond) | F.expr(cond).isNull(), F.lit(rule_name)).otherwise(None) 
                for rule_name, cond in rules_dict.items()
                ]
           
           #Write all the failed records to an array and remove nulls

            df_flagged = df.withColumns({
            'failed_rules': F.filter(F.array(*rule_checks), lambda x: x.isNotNull()),
                'quarantined_at': F.current_timestamp()
                } )

            

            df_quarantine = df_flagged.filter(F.size(F.col('failed_rules')) > 0)

            # Write to table
            df_quarantine.write.mode('append').saveAsTable(quarantine_target)
            return df_quarantine
        
        return process_clean, process_quarantine


