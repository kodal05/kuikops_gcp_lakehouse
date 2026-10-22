# Replace <handle> (twice), then commit.
project_id      = "kuikops-lakehouse"
handle          = "niahind"
service_account = "niahind-batch@kuikops-lakehouse.iam.gserviceaccount.com"
run             = 2
drop            = 1
bronze_run      = 1
silver_run      = 1
gold_run        = 1
