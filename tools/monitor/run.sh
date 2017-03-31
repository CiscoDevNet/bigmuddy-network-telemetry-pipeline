docker rm -vf prom4ppl 2>/dev/null || echo "No prometheus instance to kill"
docker rm -fv graf4ppl 2>/dev/null || echo "No grafana instance to kill"
docker run -d --net=host --name prom4ppl -v ${PWD}/data_prom4ppl:/etc/prometheus prom/prometheus:v1.5.2 -config.file=/etc/prometheus/prometheus.yml
docker run -d -i --net=host -v ${PWD}/data_graf4ppl:/var/lib/grafana --name graf4ppl grafana/grafana:4.2.0
