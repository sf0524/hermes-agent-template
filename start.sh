#!/bin/bash
set -e

# Mirror dashboard-ref-only's startup: create every directory hermes expects
# and seed a default config.yaml if the volume is empty. Without these,
# `hermes dashboard` endpoints that hit logs/, sessions/, cron/, etc. can fail
# with opaque errors even though no auth is actually involved.
# NOTE (hermes >= v2026.7.1): several dirs were consolidated and are now
# resolved via get_hermes_dir("<new>", "<old>"), which returns the NEW path
# unless the OLD one already has *content*. Seeding an empty legacy stub no
# longer "claims" it — hermes ignores empty stubs and writes to the new path
# (upstream #27602). So we seed the NEW paths: pairing -> platforms/pairing,
# image_cache -> cache/images, audio_cache -> cache/audio. A populated legacy
# dir from a pre-v2026.7.1 deploy still wins on both sides, so no migration is
# needed. server.py:_resolve_pairing_dir() mirrors this same rule for the
# admin panel's Users tab — keep the two in sync on future bumps.
mkdir -p /data/.hermes/cron /data/.hermes/sessions /data/.hermes/logs \
         /data/.hermes/memories /data/.hermes/skills /data/.hermes/platforms/pairing \
         /data/.hermes/hooks /data/.hermes/cache/images /data/.hermes/cache/audio \
         /data/.hermes/workspace /data/.hermes/skins /data/.hermes/plans \
         /data/.hermes/home

# Stamp the install method as "docker" so hermes treats this as an immutable
# container image, not a pip checkout. hermes's detect_install_method() reads
# $HERMES_HOME/.install_method FIRST (before any .git / pip fallback). Without
# this stamp the template falls through to "pip" — because the Dockerfile strips
# /opt/hermes-agent/.git — and the dashboard's "Update Hermes" button then runs
# a real `hermes update` (PyPI pip-upgrade) INSIDE the running container. That
# upgrade is ephemeral (reverts on the next redeploy) and can desync the Python
# package from the image's pre-built web_dist/ui-tui bundles. Stamping "docker"
# makes that button correctly refuse with "pull a fresh image / redeploy", which
# matches the real upgrade path here (bump HERMES_REF in Railway + redeploy).
# Written unconditionally each boot so it stays correct and self-heals.
printf 'docker\n' > /data/.hermes/.install_method

if [ ! -f /data/.hermes/config.yaml ] && [ -f /opt/hermes-agent/cli-config.yaml.example ]; then
  cp /opt/hermes-agent/cli-config.yaml.example /data/.hermes/config.yaml
fi

[ ! -f /data/.hermes/.env ] && touch /data/.hermes/.env

# Bootstrap OAuth tokens from env var (e.g. xAI Grok SuperGrok).
# Set HERMES_AUTH_JSON_BOOTSTRAP to the contents of a locally-generated
# ~/.hermes/auth.json. Written only once — subsequent token refreshes update
# the file in place on the persistent volume.
if [ ! -f /data/.hermes/auth.json ] && [ -n "${HERMES_AUTH_JSON_BOOTSTRAP}" ]; then
  printf '%s' "${HERMES_AUTH_JSON_BOOTSTRAP}" > /data/.hermes/auth.json
  chmod 600 /data/.hermes/auth.json
fi

# Clear stale gateway runtime files left over from the previous container.
# hermes writes these on start but does not remove them on SIGTERM, and /data
# is a persistent volume, so they survive into the next boot:
#   gateway.pid   -> "PID file race lost to another gateway instance"
#   gateway.lock  -> since v2026.8.27 get_running_pid() also consults the lock,
#                    and the new cross-profile gate makes `--replace` REFUSE a
#                    pid it cannot prove owns this HERMES_HOME (gateway/run.py
#                    "Refusing --replace"), which no retry can clear
#   gateway.sock  -> a stale control socket blocks the fresh bind
# No hermes process can be running here (we are pre-exec in a fresh
# container), so removing all three unconditionally is safe.
rm -f /data/.hermes/gateway.pid /data/.hermes/gateway.lock /data/.hermes/gateway.sock


# Durable lazy-install target for opt-in backends (supermemory, mem0, firecrawl, etc.).
# The template installs hermes into system Python (`uv pip install --system`) with no
# venv, so `uv pip install` at runtime fails with "No virtual environment found." Set
# HERMES_LAZY_INSTALL_TARGET to redirect runtime package installs into a writable dir
# on the persistent volume — same mechanism the official Docker image bakes in. This
# must be exported so the gateway process inherits it; hermes_bootstrap.py activates it
# at startup. Without it, any lazy dep (including opt-in providers like supermemory)
# fails on every fresh container deploy.
mkdir -p /data/.hermes/lazy-packages
export HERMES_LAZY_INSTALL_TARGET=/data/.hermes/lazy-packages

# HERMES_DASHBOARD_PUBLIC_URL is deliberately NOT exported here. server.py owns
# it: build_hermes_env() sets it only alongside the basic-auth credentials that
# satisfy hermes' auth gate. Declaring the URL without them makes the dashboard
# SystemExit at startup (v2026.8.27's should_require_dashboard_auth), and since
# Dashboard has no respawn supervisor every proxied page 503s until redeploy
# while /setup and /health stay green. Setting it here would skip that pairing.

exec python /app/server.py
