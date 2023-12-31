# beam-python-streaming
Apache beam Python SDK version 2.42. 
Beam pipeline for CDP realtime usecase - reads from pubsub > extracts required fields > send https request to vertexAI endpoint > send https request to tealium > writes data to BigQuery

## Command 
```
python3 -m pubsub-bigquery-vertexai \
--output_table="practice-project:demo_dataset.cdp_realtime_prospect_stream" \
--input_subscription="projects/practice-project/subscriptions/test_new_module_subscription" \
--runner=DataflowRunner \
--project=practice-project \
--region=europe-west1 \
--autoscaling_algorithm=THROUGHPUT_BASED \
--num_workers=6 \
--max_num_workers=500 \
--temp_location=gs://practice-project-bucket/dataflow/tmp/ \
--network=practice-project-vpc \
--subnetwork=regions/europe-west1/subnetworks/restricted-zone \
--no_use_public_ips \
--service_account_email=datafusion-worker-sa@practice-project.iam.gserviceaccount.com
```
