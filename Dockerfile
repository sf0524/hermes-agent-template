# Hermes v2026.9.21 introduced a SQLite build without the upstream WAL-reset
# corruption bug. Debian's older shared SQLite remains affected. Build the
# same pinned SQLite release and feature
# set as Hermes' official image. Both stages use the same pinned Debian base
# so the replacement shared library matches the runtime libc. PM's Python
# embeds its own fixed SQLite3.53.1; the helper verifies that interpreter too.
FROM debian:13.4@sha256:e2d08da6f42ef4b09b165d55528a12727aeed8240dc9edf888e3ec07e10ef9da AS sqlite_build
ARG SQLITE_AUTOCONF_VERSION=3530400
ARG SQLITE_SHA256=0e9483900e92cd5de8fd48d16bf9200145a61f7fd5be542a5ac81d8a9516eb9c
RUN apt-get -o Acquire::Retries=3 update && \
    apt-get -o Acquire::Retries=3 install -y --no-install-recommends \
        build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    (curl -fsSL --retry 1 --retry-all-errors --connect-timeout 15 --max-time 60 \
        -o /tmp/sqlite.tar.gz \
        "https://sqlite.org/2026/sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}.tar.gz" || \
     curl -fsSL --retry 3 --retry-all-errors --connect-timeout 15 --max-time 120 \
        -o /tmp/sqlite.tar.gz \
        "https://sources.buildroot.net/sqlite/sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}.tar.gz") && \
    printf '%s  %s\n' "${SQLITE_SHA256}" /tmp/sqlite.tar.gz > /tmp/sqlite.sha256 && \
    sha256sum -c /tmp/sqlite.sha256 && \
    tar -xzf /tmp/sqlite.tar.gz -C /tmp && \
    cd "/tmp/sqlite-autoconf-${SQLITE_AUTOCONF_VERSION}" && \
    CFLAGS="-O2 \
        -DSQLITE_ENABLE_FTS3 \
        -DSQLITE_ENABLE_FTS3_PARENTHESIS \
        -DSQLITE_ENABLE_FTS4 \
        -DSQLITE_ENABLE_FTS5 \
        -DSQLITE_ENABLE_RTREE \
        -DSQLITE_ENABLE_GEOPOLY \
        -DSQLITE_ENABLE_COLUMN_METADATA \
        -DSQLITE_ENABLE_UNLOCK_NOTIFY \
        -DSQLITE_ENABLE_DBSTAT_VTAB \
        -DSQLITE_ENABLE_DBPAGE_VTAB \
        -DSQLITE_ENABLE_MATH_FUNCTIONS \
        -DSQLITE_ENABLE_PREUPDATE_HOOK \
        -DSQLITE_ENABLE_SESSION \
        -DSQLITE_SECURE_DELETE \
        -DSQLITE_THREADSAFE=1 \
        -DSQLITE_MAX_VARIABLE_NUMBER=250000" \
        ./configure --prefix=/opt/sqlite-fixed --disable-static && \
    make -j"$(nproc)" && \
    make install

FROM debian:13.4@sha256:e2d08da6f42ef4b09b165d55528a12727aeed8240dc9edf888e3ec07e10ef9da

ARG HERMES_REF=v0.22.2
ENV HERMES_REF=${HERMES_REF} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HERMES_RUNTIME_DIR=/opt/hermes/tools \
    LD_LIBRARY_PATH=/usr/local/lib \
    CC=gcc CXX=g++ \
    npm_config_install_links=false

# The distro Python bootstraps PM; the application uses PM's checksum-verified
# Python/uv/Node/npm pins from this release's lock. Keep tini as our supervisor.
RUN apt-get -o Acquire::Retries=3 update && \
    apt-get -o Acquire::Retries=3 install -y --no-install-recommends \
        ca-certificates curl git tini python3 python-is-python3 python3-dev \
        gcc g++ make cmake libffi-dev libolm-dev libatomic1 openssh-client \
        procps xz-utils && \
    rm -rf /var/lib/apt/lists/*

COPY --from=sqlite_build /opt/sqlite-fixed/lib/libsqlite3.so.3.53.4 /usr/local/lib/
RUN ln -sf libsqlite3.so.3.53.4 /usr/local/lib/libsqlite3.so.0 && \
    ln -sf libsqlite3.so.3.53.4 /usr/local/lib/libsqlite3.so && \
    printf '/usr/local/lib\n' > /etc/ld.so.conf.d/000-sqlite-fixed.conf && \
    ldconfig

# A parent payload manifest is required by PM's fresh/fallback child launchers.
# /opt/hermes-agent remains available as a compatibility link after assembly.
RUN git clone --depth 1 --branch "${HERMES_REF}" \
        https://github.com/NousResearch/hermes-agent.git /opt/hermes/repo && \
    if [ "${HERMES_REF}" = v0.22.2 ]; then \
        test "$(git -C /opt/hermes/repo rev-parse HEAD)" = \
            d923a5eb487a17c0a68801bba9ef0f56c81040a8; \
    fi
WORKDIR /opt/hermes/repo
COPY template_helpers/build_hermes_image.py /app/template_helpers/build_hermes_image.py
COPY requirements.txt /app/requirements.txt
RUN python3 /app/template_helpers/build_hermes_image.py tools

# Freeze the declared release graph and its exact baked-extra baseline. No raw
# pip/uv mutations follow it. The retired honcho extra belongs to its plugin.
RUN python3 /app/template_helpers/build_hermes_image.py dependencies
ENV HERMES_PYTHON=/opt/hermes/venv/bin/python
RUN python3 /app/template_helpers/build_hermes_image.py frontends && \
    python3 /app/template_helpers/build_hermes_image.py assemble && \
    python3 /app/template_helpers/build_hermes_image.py verify && \
    rm -rf /root/.cache /root/.hermes /root/.npm

# The admin server uses only the image-owned wrapper/base environment. PM may
# select new application generations without changing its dependency imports.
ENV PATH="/opt/hermes/bin:/opt/hermes/wrapper-venv/bin:${PATH}" \
    HOME=/data \
    HERMES_HOME=/data/.hermes \
    HERMES_TUI_DIR=/opt/hermes/ui-tui
RUN mkdir -p /data/.hermes
COPY server.py /app/server.py
COPY template_helpers/ /app/template_helpers/
COPY templates/ /app/templates/
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

ENTRYPOINT ["/usr/bin/tini", "-g", "--"]
CMD ["/app/start.sh"]
