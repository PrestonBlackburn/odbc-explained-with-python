# setup postgres container with no auth
docker run -n postgres_test -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:latest