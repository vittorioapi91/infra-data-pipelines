pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Validate Airflow DAGs') {
            when {
                expression { fileExists('src/_airflow_dags_') }
            }
            steps {
                sh '''
                    if [ ! -d "venv" ]; then
                        python3 -m venv venv
                    fi
                    VENV_PYTHON="venv/bin/python"
                    VENV_PIP="venv/bin/pip"

                    ${VENV_PIP} install --quiet --upgrade pip
                    ${VENV_PIP} install --quiet apache-airflow || ${VENV_PIP} install --quiet 'apache-airflow>=2.0.0'
                    ${VENV_PIP} install --quiet tqdm pandas psycopg2-binary requests python-dotenv

                    export AIRFLOW_HOME=/tmp/airflow_home
                    export AIRFLOW__CORE__DAGS_FOLDER=src/_airflow_dags_
                    export AIRFLOW__CORE__LOAD_EXAMPLES=False
                    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow_home/airflow.db

                    mkdir -p ${AIRFLOW_HOME}
                    ${VENV_PYTHON} -m airflow db migrate || ${VENV_PYTHON} -m airflow db init
                    ${VENV_PYTHON} -m airflow dags list
                '''
            }
        }

        stage('Run Tests') {
            steps {
                sh '''
                    if [ ! -d "venv" ]; then
                        python3 -m venv venv
                    fi
                    VENV_PYTHON="venv/bin/python"
                    VENV_PIP="venv/bin/pip"

                    ${VENV_PIP} install --quiet --upgrade pip

                    if [ "${BRANCH_NAME}" = "staging" ]; then
                        REQ_FILE="requirements-staging.txt"
                    elif [ "${BRANCH_NAME}" = "main" ]; then
                        REQ_FILE="requirements-prod.txt"
                    else
                        REQ_FILE="requirements-dev.txt"
                    fi
                    if [ ! -f "${REQ_FILE}" ]; then
                        REQ_FILE="requirements.txt"
                    fi

                    ${VENV_PIP} install --quiet -r ${REQ_FILE}
                    mkdir -p test-results
                    set +e
                    ${VENV_PYTHON} -m pytest tests/ --junitxml=test-results/junit.xml --html=test-results/report.html --self-contained-html
                    TEST_EXIT_CODE=$?
                    set -e
                    exit ${TEST_EXIT_CODE}
                '''
            }
            post {
                always {
                    junit 'test-results/junit.xml'
                    publishHTML([
                        reportName: 'Test Report',
                        reportDir: 'test-results',
                        reportFiles: 'report.html',
                        keepAll: true,
                        alwaysLinkToLastBuild: true,
                        allowMissing: true
                    ])
                }
            }
        }
    }
}
