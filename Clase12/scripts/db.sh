#!/usr/bin/env bash
# Postgres de la demo sobre podman, sin depender de docker ni de podman-compose.
#   ./scripts/db.sh up | down | reset | psql | logs | status
set -euo pipefail

CONTAINER=postgres_r2dbc
IMAGE=docker.io/library/postgres:15
VOLUME=pgdata
DB=testdb
USER=postgres
PASS=postgres
PORT=5432

need_podman() {
  command -v podman >/dev/null || { echo "podman no está en el PATH"; exit 1; }
  podman machine inspect podman-machine-default >/dev/null 2>&1 || return 0
  if ! podman info >/dev/null 2>&1; then
    echo "Arrancando la máquina de podman..."
    podman machine start
  fi
}

up() {
  need_podman
  if podman container exists "$CONTAINER"; then
    podman start "$CONTAINER" >/dev/null
  else
    podman run -d --name "$CONTAINER" \
      -p "${PORT}:5432" \
      -e POSTGRES_DB="$DB" -e POSTGRES_USER="$USER" -e POSTGRES_PASSWORD="$PASS" \
      -v "${VOLUME}:/var/lib/postgresql/data" \
      "$IMAGE" >/dev/null
  fi
  printf 'Esperando a Postgres'
  for _ in $(seq 1 60); do
    if podman exec "$CONTAINER" pg_isready -U "$USER" -d "$DB" >/dev/null 2>&1; then
      echo " listo en localhost:${PORT}/${DB}"; return 0
    fi
    printf '.'; sleep 1
  done
  echo; echo "Postgres no respondió a tiempo"; podman logs --tail 20 "$CONTAINER"; exit 1
}

down()   { need_podman; podman stop "$CONTAINER" >/dev/null 2>&1 || true; echo "detenido"; }
reset()  { need_podman; podman rm -f "$CONTAINER" >/dev/null 2>&1 || true
           podman volume rm "$VOLUME" >/dev/null 2>&1 || true; up; }
psql()   { need_podman
           # -t solo si hay terminal: permite `./scripts/db.sh psql -c "SELECT 1"` en scripts/CI
           local flags=(-i); [ -t 0 ] && [ -t 1 ] && flags=(-i -t)
           podman exec "${flags[@]}" "$CONTAINER" psql -U "$USER" -d "$DB" "$@"; }
logs()   { need_podman; podman logs -f "$CONTAINER"; }
status() { need_podman; podman ps --filter "name=${CONTAINER}"; }

cmd="${1:-up}"; shift || true
case "$cmd" in
  up|down|reset|psql|logs|status) "$cmd" "$@" ;;
  *) echo "uso: $0 {up|down|reset|psql|logs|status}"; exit 2 ;;
esac
