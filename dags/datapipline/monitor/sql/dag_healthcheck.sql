with dag_metadata (dag_id, tasks, max_run_date) as
(select dag_id, max(tasks) tasks, max_run_date::date
from
(
select d.dag_id, count(distinct task_id) tasks, max(d.execution_date) max_run_date
from dag_run d left outer join task_instance t
on d.dag_id = t.dag_id
where t.start_date::date >= now()::date - 7
group by d.dag_id
) s
group by s.dag_id, s.max_run_date
)

select d.dag_id, 'UNDEFINED' use_case
from dag_run d join dag_metadata m
on d.dag_id = m.dag_id
left outer join task_instance t
on t.execution_date::date = d.execution_date::date and t.dag_id = d.dag_id
where d.execution_date::date = m.max_run_date and d.state not in ('success', 'running')
group by d.dag_id
having count(t.task_id) < max(tasks)

union all

select t.dag_id, 'UNSUCCESSFUL' use_case
from task_instance t, dag_run d, dag_metadata m
where t.dag_id = d.dag_id and t.execution_date::date = d.execution_date::date and d.dag_id = m.dag_id and d.execution_date::date = m.max_run_date
and t.state not in ('success', 'running', 'queued', 'up_for_retry')
group by t.dag_id

union all

select d.dag_id, 'NOT RUNNING' use_case
from dag_run d join dag_metadata m
on d.dag_id = m.dag_id
left outer join task_instance t
on t.execution_date::date = d.execution_date::date and t.dag_id = d.dag_id
where d.execution_date::date = m.max_run_date and d.state = 'running'
group by d.dag_id
having count(t.state) = 0
