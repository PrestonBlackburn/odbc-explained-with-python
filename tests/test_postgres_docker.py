from python_on_whales import docker
import pytest
import logging

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
        detatch=True,
        remove=True,
        interactive=False,
    )

    yield True
    docker.stop("postgres_auto_test")


