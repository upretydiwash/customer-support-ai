from pyspark.sql import functions as F

class DataQualityPipeline:
    def __init__(self, df, quarantine_target, rules: dict):
        self.df = df
        self.quarantine_target = quarantine_target
        self.rules = rules  # e.g., {"valid_id": "customer_id IS NOT NULL"}

    def data_quality(self):
        # Extract variables into local scope for closure evaluation
        #Mapping each row to a case statement: if rule evaluated to FALSE/NULL, return rule_name
        rules_dict = self.rules
        rule_checks = [ 
            F.when(~F.expr(cond) | F.expr(cond).isNull(), F.lit(rule_name)).otherwise(None) 
            for rule_name, cond in rules_dict.items()
            ]
        #Write all the failed records to an array and remove nulls
        df_flagged = self.df.withColumns({
            'failed_rules': F.filter(F.array(*rule_checks), lambda x: x.isNotNull()),
                'quarantined_at': F.current_timestamp()
                } )
        df_quarantine = df_flagged.filter(F.size(F.col('failed_rules')) > 0)
        df_clean = df_flagged.filter(F.size(F.col('failed_rules')) == 0)
        # Write quarantine to table
        df_quarantine.write.mode('append').saveAsTable(self.quarantine_target)
        return df_clean, df_quarantine
    

           


            


        
        

