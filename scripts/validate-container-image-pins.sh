#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${DBTOOL_COMPOSE_FILE:-$ROOT/docker-compose.integration.yml}"

if (($# > 0)); then
  COMPOSE_FILE="$1"
  shift
fi

if (($# > 0)); then
  DOCKERFILES=("$@")
else
  DOCKERFILES=(
    "$ROOT/docker/dbtool/Dockerfile"
    "$ROOT/docker/fixtures/mongo/Dockerfile"
    "$ROOT/docker/fixtures/mysql/Dockerfile"
    "$ROOT/docker/fixtures/postgres/Dockerfile"
    "$ROOT/docker/fixtures/redis/Dockerfile"
    "$ROOT/docker/search-tls/Dockerfile"
  )
fi

fail() {
  echo "container image pin validation failed: $*" >&2
  exit 1
}

strip_quotes() {
  local value="$1"
  if [[ "$value" == \"*\" && "$value" == *\" ]]; then
    value="${value#\"}"
    value="${value%\"}"
  elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
    value="${value#\'}"
    value="${value%\'}"
  fi
  printf '%s\n' "$value"
}

resolve_compose_default() {
  local value="$1"
  local prefix default suffix

  while [[ "$value" =~ ^(.*)\$\{[A-Za-z_][A-Za-z0-9_]*:-([^\}]*)\}(.*)$ ]]; do
    prefix="${BASH_REMATCH[1]}"
    default="${BASH_REMATCH[2]}"
    suffix="${BASH_REMATCH[3]}"
    value="${prefix}${default}${suffix}"
  done

  [[ "$value" != *'${'* ]] || fail "image expression has no deterministic default: $1"
  printf '%s\n' "$value"
}

validate_remote_ref() {
  local ref="$1"
  local context="$2"

  [[ "$ref" != "scratch" ]] || return 0
  if [[ "$ref" == *:latest ]]; then
    fail "$context uses latest without a sha256 digest: $ref"
  fi
  if [[ "$ref" != *@sha256:* ]]; then
    fail "$context remote image default is not pinned: $ref"
  fi
  if [[ ! "$ref" =~ ^[^[:space:]@]+:[^[:space:]@/]+@sha256:[0-9a-f]{64}$ ]]; then
    fail "$context has malformed image pin: $ref"
  fi
}

finalize_service() {
  [[ -n "${current_service:-}" && -n "${current_image:-}" ]] || return 0
  if [[ "$current_image" == *:local && "$current_has_build" -ne 1 ]]; then
    fail "compose service $current_service local image default requires build: $current_image"
  fi
}

validate_compose() {
  local line raw resolved
  local current_service=""
  local current_image=""
  local current_has_build=0
  local image_count=0

  [[ -f "$COMPOSE_FILE" ]] || fail "missing compose file $COMPOSE_FILE"

  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^\ \ ([A-Za-z0-9._-]+):[[:space:]]*$ ]]; then
      finalize_service
      current_service="${BASH_REMATCH[1]}"
      current_image=""
      current_has_build=0
      continue
    fi

    if [[ "$line" =~ ^[[:space:]]{4}image:[[:space:]]*(.+)$ ]]; then
      [[ -n "$current_service" ]] || fail "image is not inside a compose service"
      raw="$(strip_quotes "${BASH_REMATCH[1]}")"
      resolved="$(resolve_compose_default "$raw")"
      current_image="$resolved"
      image_count=$((image_count + 1))
      if [[ "$resolved" != *:local ]]; then
        validate_remote_ref "$resolved" "compose service $current_service"
      fi
      continue
    fi

    if [[ "$line" =~ ^[[:space:]]{4}build: ]]; then
      current_has_build=1
    fi
  done <"$COMPOSE_FILE"

  finalize_service
  ((image_count > 0)) || fail "compose file has no service images: $COMPOSE_FILE"
}

lookup_arg_default() {
  local name="$1"
  local index
  for ((index = 0; index < ${#arg_names[@]}; index++)); do
    if [[ "${arg_names[$index]}" == "$name" ]]; then
      printf '%s\n' "${arg_values[$index]}"
      return 0
    fi
  done
  return 1
}

resolve_dockerfile_default() {
  local ref="$1"
  local name default token

  while [[ "$ref" =~ \$\{([A-Za-z_][A-Za-z0-9_]*)\} ]]; do
    name="${BASH_REMATCH[1]}"
    default="$(lookup_arg_default "$name")" || fail "Dockerfile ARG $name has no default"
    token="\${${name}}"
    ref="${ref//$token/$default}"
  done

  [[ "$ref" != *'$'* ]] || fail "Dockerfile FROM has an unresolved build argument: $ref"
  printf '%s\n' "$ref"
}

validate_dockerfile() {
  local dockerfile="$1"
  local line name value from_payload first second ref
  local from_count=0
  arg_names=()
  arg_values=()

  [[ -f "$dockerfile" ]] || fail "missing Dockerfile $dockerfile"

  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^[[:space:]]*ARG[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)=(.+)$ ]]; then
      name="${BASH_REMATCH[1]}"
      value="$(strip_quotes "${BASH_REMATCH[2]}")"
      arg_names+=("$name")
      arg_values+=("$value")
      continue
    fi

    if [[ "$line" =~ ^[[:space:]]*FROM[[:space:]]+(.+)$ ]]; then
      from_payload="${BASH_REMATCH[1]}"
      read -r first second _ <<<"$from_payload"
      if [[ "$first" == --platform=* ]]; then
        [[ -n "${second:-}" ]] || fail "$dockerfile has an invalid FROM instruction: $line"
        ref="$second"
      else
        ref="$first"
      fi
      ref="$(resolve_dockerfile_default "$ref")"
      from_count=$((from_count + 1))
      if [[ "$ref" != *:local ]]; then
        validate_remote_ref "$ref" "Dockerfile $dockerfile"
      fi
    fi
  done <"$dockerfile"

  ((from_count > 0)) || fail "Dockerfile has no FROM instruction: $dockerfile"
}

validate_compose
for dockerfile in "${DOCKERFILES[@]}"; do
  validate_dockerfile "$dockerfile"
done

echo "container image pin validation passed (${#DOCKERFILES[@]} Dockerfiles)"
