from airflow.models import DagBag
import pytest

def test_dag_loaded():
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import failures: {dagbag.import_errors}"
    
def test_dag_structure():
    dagbag = DagBag(dag_folder='dags', include_examples=False)
    dag = dagbag.get_dag(dag_id='sales_data_generator')
    
    # Test basic properties
    assert dag is not None
    assert dag.schedule_interval == '*/30 * * * *'
    
    # Test task dependencies
    tasks = dag.tasks
    task_ids = [task.task_id for task in tasks]
    
    assert 'generate_data' in task_ids
    assert 'produce_to_kafka' in task_ids
    assert 'save_records_to_s3' in task_ids
    assert 'cleanup' in task_ids
