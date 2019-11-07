#!/bin/sh

export RANKDB_USERNAME=${RANKDB_USERNAME:-"rankdb"}
export RANKDB_GROUPNAME=${RANKDB_GROUPNAME:-"rankdb"}


docker_set_uid_gid() {
    addgroup -S "$RANKDB_GROUPNAME" >/dev/null 2>&1 && \
        adduser -S -G "$RANKDB_GROUPNAME" "$RANKDB_USERNAME" >/dev/null 2>&1
}

# su-exec to requested user, if user cannot be requested
# existing user is used automatically.
docker_switch_user() {
    owner=$(check-user "$@")
    if [ "${owner}" != "${RANKDB_USERNAME}:${RANKDB_GROUPNAME}" ]; then
        ## Print the message only if we are not using non-default username:groupname.
        if [ "${RANKDB_USERNAME}:${RANKDB_GROUPNAME}" != "rankdb:rankdb" ]; then
            echo "Requested username/group ${RANKDB_USERNAME}:${RANKDB_GROUPNAME} cannot be used"
            echo "Found existing data with user ${owner}, we will continue and use ${owner} instead."
            return
        fi
    fi
    # check if su-exec is allowed, if yes proceed proceed.
    if su-exec "${owner}" "/bin/ls" >/dev/null 2>&1; then
        exec su-exec "${owner}" "$@"
    fi
    # fallback
    exec "$@"
}

if [ "${1}" != "rankdb" ] && [ "${1}" != "rankdb-cli" ]; then
    if [ -n "${1}" ]; then
        set -- rankdb "$@"
    fi
fi

## User Input UID and GID
docker_set_uid_gid

## Switch to user if applicable.
docker_switch_user "$@"

