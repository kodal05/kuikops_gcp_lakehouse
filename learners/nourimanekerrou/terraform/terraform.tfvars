# Replace <handle> (twice), then commit.
project_id      = "kuikops-lakehouse"
handle          = "nourimanekerrou"
service_account = "nourimanekerrou-batch@kuikops-lakehouse.iam.gserviceaccount.com"
run             = 2
drop            = 2
bronze_run      = 2
silver_run      = 1
gold_run        = 1