#! /bin/bash
# MAKE SURE GCP PROJECT IS SET
# gcloud config set project PROJECT_ID
gcloud config set project playground-s-11-2f33e9e8
if [[ -z "${GOOGLE_CLOUD_PROJECT}" ]]; then
    echo "Project has not been set! Please run:"
    echo "   gcloud config set project PROJECT_ID"
    echo "(where PROJECT_ID is the desired project)"
else
    echo "Project Name: $GOOGLE_CLOUD_PROJECT"
    gcloud storage buckets create gs://$GOOGLE_CLOUD_PROJECT"-bucket" --soft-delete-duration=0
    gcloud services enable dataflow.googleapis.com
    gcloud iam service-accounts create marssa
    sleep 1
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member serviceAccount:marssa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com --role roles/editor
    sleep 1
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member serviceAccount:marssa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com --role roles/dataflow.worker
    sleep 1
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member user:$USER_EMAIL --role roles/iam.serviceAccountUser
    bq mk mars
    bq mk --schema timestamp:STRING,ipaddr:STRING,action:STRING,srcacct:STRING,destacct:STRING,amount:NUMERIC,customername:STRING -t mars.activities
fi
