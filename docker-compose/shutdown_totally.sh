docker compose down
docker volume rm $(docker volume ls -q )
docker rmi hadoop-zmd01 hadoop-zmd02 hadoop-zmd03