from python_on_whales import docker
import pytest
import logging
import requests
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@pytest.fixture(scope="session", autouse=True)
def start_postgres():
    # docker run -n postgres_test -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:latest
    output = docker.run(
        "postgres:latest",
        name="postgres_auto_test",
        envs = {
            "POSTGRES_HOST_AUTH_METHOD": "trust"
        },
        publish=[(5432, 5432)],
        detach=True,
        remove=True,
        interactive=False,
    )

    yield True
    docker.stop("postgres_auto_test")


@pytest.fixture(scope="session")
def wait_for_postgres(start_postgres):
    # docker exec pg_isready -h localhost -p 5432 -U postgres
    timeout = 15
    while timeout > 0:
        try:
            output = docker.execute(
                "postgres_auto_test",
                ["pg_isready", "-h", "localhost", "-p", "5432", "-U", "postgres"],
            )
            if "accepting connections" in output:
                break
        except Exception as e:
            print(e)
            pass
        timeout -= 1
        time.sleep(1)
    if timeout == 0:
        pytest.fail("API did not become available within the timeout") 
