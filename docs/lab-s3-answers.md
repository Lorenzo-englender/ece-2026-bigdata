# Lab 2 - S3 answers

**Why are the credentials stored in a Secret and not in the ConfigMap?**
A ConfigMap is for regular config and anyone in the namespace can read it. A Secret is made for sensitive data: access can be restricted and it can be encrypted in the cluster. Access keys shouldn't be in a ConfigMap.

**What happens if the Job is executed again tomorrow? How would a production platform provide credentials?**
The Onyxia credentials are temporary, so tomorrow they will be expired and the upload will fail with an ExpiredToken error. In production we don't copy keys by hand, the pod gets its own identity (a service account linked to an IAM role, like IRSA on AWS) and the credentials are provided and renewed automatically.

**How would you turn this Job into a daily ingestion?**
Turn it into a Kubernetes CronJob with a schedule like `0 2 * * *` (every day at 2am), or trigger it from an orchestrator like Airflow.
