queries = [
"""
UPDATE wmbs_job SET wmbs_job.state = (SELECT id FROM wmbs_job_state where name = 'complete')
WHERE wmbs_job.id IN (
SELECT wmbs_job.id
FROM wmbs_job
INNER JOIN wmbs_job_state ON
wmbs_job.state = wmbs_job_state.id
LEFT OUTER JOIN bl_runjob ON
wmbs_job.id = bl_runjob.wmbs_id
LEFT OUTER JOIN bl_status ON
bl_runjob.sched_status = bl_status.id
INNER JOIN wmbs_jobgroup ON
wmbs_job.jobgroup = wmbs_jobgroup.id
INNER JOIN wmbs_subscription ON
wmbs_jobgroup.subscription = wmbs_subscription.id
INNER JOIN wmbs_workflow ON
wmbs_subscription.workflow = wmbs_workflow.id
WHERE wmbs_workflow.name = 'nnazirid_HIN-HiWinter13-00001_10_v1__121121_170049_1345'
AND wmbs_job_state.name = 'executing'
AND bl_runjob.status = 0
GROUP BY wmbs_job.id
)
""",

"""
UPDATE wmbs_job SET wmbs_job.state = (SELECT id FROM wmbs_job_state where name = 'complete')
WHERE wmbs_job.id = 30045
""",
"""
SELECT rj.wmbs_id jobid, rj.grid_id gridid, rj.bulk_id bulkid,
               st.name status, rj.retry_count retry_count, rj.id id,
               rj.status_time status_time, wu.cert_dn AS userdn,
               wu.group_name AS usergroup, wu.role_name AS userrole,
               wj.cache_dir AS cache_dir
             FROM bl_runjob rj
             INNER JOIN bl_status st ON rj.sched_status = st.id
             LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
             INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
             WHERE rj.status = 1 AND cache_dir like '/data/srv/wmagent/v0.9.13/install/wmagent/JobCreator/JobCache/nnazirid_HIN-HiWinter13-00001_10_v1__121121_170049_1345%'
             """,
"""
UPDATE wmbs_job SET wmbs_job.state = (SELECT id FROM wmbs_job_state where name = 'complete')
WHERE wmbs_job.id IN (
SELECT wmbs_job.id
FROM wmbs_job
INNER JOIN wmbs_job_state ON
wmbs_job.state = wmbs_job_state.id
LEFT OUTER JOIN bl_runjob ON
wmbs_job.id = bl_runjob.wmbs_id
LEFT OUTER JOIN bl_status ON
bl_runjob.sched_status = bl_status.id
INNER JOIN wmbs_jobgroup ON
wmbs_job.jobgroup = wmbs_jobgroup.id
INNER JOIN wmbs_subscription ON
wmbs_jobgroup.subscription = wmbs_subscription.id
INNER JOIN wmbs_workflow ON
wmbs_subscription.workflow = wmbs_workflow.id
HAVING MAX(bl_runjob.retry_count) != wmbs_job.retry_count AND
       wmbs_job_state.name = 'executing'
GROUP BY wmbs_job.id, wmbs_job.retry_count, wmbs_job_state.name, wmbs_workflow.name)
""",
""" 
SELECT rj.wmbs_id jobid, rj.grid_id gridid, rj.bulk_id bulkid,
               st.name status, rj.retry_count retry_count, rj.id id,
               rj.status_time status_time, wu.cert_dn AS userdn,
               wu.group_name AS usergroup, wu.role_name AS userrole,
               wj.cache_dir AS cache_dir
             FROM bl_runjob rj
INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
             INNER JOIN bl_status st ON rj.sched_status = st.id
             LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
WHERE wj.id = 30045     
        """
             
           ]