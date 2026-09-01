import dlt
from pyspark.sql import functions as F

class DataQualityPipeline:
    def __init__(self, config: dict):
        self.source_table = config.get("source_table")
        self.clean_table = config.get("clean_table")
        self.quarantine_table = config.get("quarantine_table")
        self.rules = config.get("rules", {})  # e.g., {"valid_id": "customer_id IS NOT NULL"}

    def build(self):
        # Extract variables into local scope for closure evaluation
        source = self.source_table
        clean_target = self.clean_table
        quarantine_target = self.quarantine_table
        rules_dict = self.rules

        # -------------------------------------------------------------
        # 1. CLEAN TABLE: Drops bad records using expectations
        # -------------------------------------------------------------
        @dlt.table(name=clean_target)
        @dlt.expect_all_or_drop(rules_dict)
        def process_clean():
            return dlt.read(source)

        # -------------------------------------------------------------
        # 2. QUARANTINE TABLE: Filters for rows that fail ANY rule
        # -------------------------------------------------------------
        @dlt.table(name=quarantine_target)
        def process_quarantine():
            df = dlt.read(source)
            
            # Combine all inverse rules into a single OR filter condition
            # If a row fails Rule A OR Rule B, it goes to Quarantine
            failed_conditions = [f"NOT ({cond})" for cond in rules_dict.values()]
            combined_failure_filter = " OR ".join(failed_conditions)
            
            return (
                df.filter(combined_failure_filter)
                .withColumn("quarantined_at", F.current_timestamp())
            )