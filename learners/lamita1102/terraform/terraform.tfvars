# Replace <handle> (twice), then commit.
project_id      = "kuikops-lakehouse"
handle          = "lamita1102"
service_account = "lamita1102-batch@kuikops-lakehouse.iam.gserviceaccount.com"
run             = 2
drop            = 2
bronze_run      = 2
silver_run      = 1
gold_run        = 1
