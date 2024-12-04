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
    assert dag.schedule_interval == '*/70 * * * *'  # Matches the schedule in your DAG
    
    # Test task dependencies
    tasks = dag.tasks
    task_ids = [task.task_id for task in tasks]
    
    # Check if all required tasks exist
    expected_tasks = {'generate_data', 'produce_to_kafka', 'save_records_to_s3', 'cleanup'}
    assert set(task_ids) == expected_tasks
    
    # Test task order
    generate_data = dag.get_task('generate_data')
    produce_to_kafka = dag.get_task('produce_to_kafka')
    save_records_to_s3 = dag.get_task('save_records_to_s3')
    cleanup = dag.get_task('cleanup')
    
    # Test dependencies
    assert produce_to_kafka in generate_data.downstream_list
    assert save_records_to_s3 in produce_to_kafka.downstream_list
    assert cleanup in save_records_to_s3.downstream_list