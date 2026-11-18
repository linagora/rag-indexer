#!/usr/bin/env bash
# Boots the image as in production (read-only root filesystem, no capability) next to
# a RabbitMQ, then checks it declares its queues, serves its probe and metrics and
# stops cleanly.
#
#   docker/smoke-test.sh rag-indexer:dev
set -euo pipefail

IMAGE="${1:?usage: $0 <image>}"
RUN="rag-indexer-smoke-$$"
cleanup() {
  docker rm -f "$RUN" "$RUN-rabbitmq" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The service joins the broker's network namespace, so its port is published
# here and no network has to be created.
# The rabbitmq image sets no USER, so without --user the broker runs as root.
docker run -d --name "$RUN-rabbitmq" --user 999:999 \
  -p 127.0.0.1::8080 \
  rabbitmq:3-management-alpine >/dev/null
for _ in $(seq 1 60); do
  docker exec "$RUN-rabbitmq" rabbitmq-diagnostics -q check_port_connectivity \
    >/dev/null 2>&1 && break
  sleep 1
done

docker run -d --name "$RUN" --network "container:$RUN-rabbitmq" \
  --read-only --tmpfs /tmp --cap-drop ALL --security-opt no-new-privileges \
  -e RABBITMQ_URL="amqp://guest:guest@localhost:5672/" \
  -e RAG_BASE_URL=http://openrag.invalid \
  -e RAG_API_KEY=smoke \
  "$IMAGE" >/dev/null
API="http://$(docker port "$RUN-rabbitmq" 8080/tcp | head -1)"

failures=0
expect() {
  # shellcheck disable=SC2053 # $3 is a glob pattern
  if [[ "$2" == $3 ]]; then
    echo "ok   $1"
  else
    echo "FAIL $1: got '$2'"
    failures=$((failures + 1))
  fi
}
status() { curl -s -o /dev/null -w '%{http_code}' "$@" || true; }

for _ in $(seq 1 60); do
  [[ "$(status "$API/health")" == 200 ]] && break
  sleep 1
done

queues="$(docker exec "$RUN-rabbitmq" rabbitmqctl -q list_queues name type)"
queue_types() { awk -v re="$1" '$1 ~ re { print $2 }' <<<"$queues" | sort -u | xargs; }

expect 'healthy once started' "$(status "$API/health")" '200'
expect 'runs as uid 999' "$(docker exec "$RUN" stat -c %u /proc/1)" '999'
expect 'metrics served' "$(status "$API/metrics")" '200'
expect 'messages counter exported' \
  "$(curl -s "$API/metrics" || true)" '*rag_indexer_messages_total*'
expect 'main queue is quorum' "$(queue_types '^rag\\.index\\.q$')" 'quorum'
expect 'dead-letter queue is quorum' "$(queue_types '^rag\\.index\\.dlq$')" 'quorum'
expect 'retry queues are classic' "$(queue_types '^rag\\.index\\.retry\\.')" 'classic'

docker kill -s TERM "$RUN" >/dev/null
code="$(timeout 40 docker wait "$RUN" || echo timeout)"
expect 'stops on SIGTERM with code 0' "$code" '0'

if ((failures > 0)); then
  echo "$failures check(s) failed; logs:"
  docker logs "$RUN" 2>&1 | tail -40
  exit 1
fi
echo "all checks passed"
