FROM registry.access.redhat.com/ubi9/ubi

# Install openjdk
RUN yum install -y java-17-openjdk-headless && \
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))" && \
    echo "$JAVA_HOME/lib/server" | tee /etc/ld.so.conf.d/jdk.conf && \
    ldconfig

# Install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum install -y postgresql15-server
ENV ANSILO_PG_INSTALL_DIR=/usr/pgsql-15/
# Copy default postgresql.conf
COPY .github/workflows/docker/release.postgresql.conf /ansilo/postgresql.conf
ENV ANSILO_PG_DEFAULT_CONF=/ansilo/postgresql.conf

# Install openssl
RUN yum install -y openssl

# Copy artifacts
COPY artifacts/ /ansilo/

# Set up runtime user
RUN adduser ansilo -u 1000 && \
    chown -R ansilo:ansilo /ansilo/

# Install postgres extension
RUN cp -vr /ansilo/pgx/* / && rm -rf /ansilo/pgx

# Create symlinks
RUN ln -s /ansilo/ansilo-main /usr/bin/ansilo 

# Set up user folders
RUN mkdir /app/ && \
    chown -R ansilo:ansilo /app/

# Set up default app folders
RUN mkdir -p /var/run/ansilo/ && \
    chown -R ansilo:ansilo /var/run/ansilo/

# Clean up
RUN yum clean all && \
    rpm -q java-17-openjdk-headless postgresql15-server openssl && \
    rm -rf /var/cache/yum && \
    rm -rf /tmp/*

USER ansilo
ENTRYPOINT [ "ansilo" ]
CMD [ "run" ]