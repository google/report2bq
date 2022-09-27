from typing import List
from classes.scheduler import Scheduler
from google.cloud.scheduler import (CloudSchedulerClient, CreateJobRequest,
                                    DeleteJobRequest, Job, ListJobsRequest,
                                    PauseJobRequest, PubsubTarget,
                                    ResumeJobRequest)

scheduler = Scheduler()
scheduler.email='davidharcombe@google.com'
scheduler.project='report2bq-zz9-plural-z-alpha'

# result = scheduler.list_locations()

# result: List[Job] = scheduler.process(**{
#     'action': 'list',
#     'project': 'report2bq-zz9-plural-z-alpha',
#     'email': 'davidharcombe@google.com'})
# for r in result:
#   print(Job.to_dict(r))

# result = scheduler.process(**{
#     'action': 'disable',
#     'job_id': 'sa360_hourly_depleted_20700000001309869_21700000001675457',
#     'project': 'report2bq-zz9-plural-z-alpha',
#     'email': 'davidharcombe@google.com'})

# result = scheduler.process(**{
#     'action': 'enable',
#     'job_id': 'sa360_hourly_depleted_20700000001309869_21700000001675457',
#     'project': 'report2bq-zz9-plural-z-alpha',
#     'email': 'davidharcombe@google.com'})

result = scheduler.process(**{
    'action': 'get',
    'job_id': 'fetch-cm-677362425',
    'project': 'report2bq-zz9-plural-z-alpha',
    'email': 'davidharcombe@google.com'})

print(result)
