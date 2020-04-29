import yaql
from yaql import yaqlization
from yaql.language import contexts

from taro.job import JobInfo
from taro.execution import ExecutionLifecycle

engine = yaql.factory.YaqlFactory().create()
exp = engine('$job.lifecycle.state()')
ctx = yaql.create_context()
job = JobInfo('j', 'i', ExecutionLifecycle(), '', None)
yaqlization.yaqlize(job)
ctx['job'] = job
res = exp.evaluate(context=ctx)
print(type(res))
