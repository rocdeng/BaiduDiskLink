TAG=$(git rev-parse --short HEAD)

docker compose build
docker tag baidudisklink:latest 192.168.1.5:35000/baidudisklink:$TAG
docker tag baidudisklink:latest 192.168.1.5:35000/baidudisklink:latest

docker push 192.168.1.5:35000/baidudisklink:$TAG
docker push 192.168.1.5:35000/baidudisklink:latest

