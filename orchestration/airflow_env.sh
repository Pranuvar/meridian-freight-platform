# Source it (do not execute it):   source orchestration/airflow_env.sh
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AIRFLOW_HOME="$ROOT/orchestration/airflow_home"            # Airflow's database, logs and config live here
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/orchestration/dags"      # where it looks for DAG files
export AIRFLOW__CORE__LOAD_EXAMPLES=False                         # no sample DAGs
source "$ROOT/.venv-airflow/bin/activate"
