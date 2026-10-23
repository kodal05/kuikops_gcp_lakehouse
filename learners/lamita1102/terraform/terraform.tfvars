# Replace <handle> (twice), then commit.
project_id      = "kuikops-lakehouse"
handle          = "lamita1102"
service_account = "lamita1102-batch@kuikops-lakehouse.iam.gserviceaccount.com"
run             = 2
drop            = 3
bronze_run      = 3
silver_run      = 3
gold_run        = 1
