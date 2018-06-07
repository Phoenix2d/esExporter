really dumb **es -> s3** index exporter

was modelled to be a lambda so for external config relies on env variables
requires these to be set to operate:

* ES_ENDPOINT
* ES_ENDPOINT_PORT
* AWS_ACCESS_KEY_ID  
* AWS_SECRET_KEY
* REGION

by default streams indices to **s3://k8s-tenant-logs**

