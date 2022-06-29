# Creates an oracle image which is tuned for fast start up
FROM oracle-base

ENV ORACLE_SID=ansilo
ENV ORACLE_PDB=db
ENV ORACLE_PWD=ansilo_admin_pass
ENV ORACLE_EDITION=enterprise
ENV ORACLE_CHARACTERSET=AL32UTF8

COPY ./sql/* /opt/oracle/scripts/startup/ 
USER root
RUN chown -R oracle:dba /opt/oracle/scripts/startup/
USER oracle

# Start up oracle (last startup script should cause it to terminate)
RUN bash -c 'grep -m1 -qe "Instance shutdown complete" <($ORACLE_BASE/$RUN_FILE 2>&1 | tee /dev/stderr)' && \
    echo "Detected shutdown"

# Remove startup scripts
RUN rm -rf /opt/oracle/scripts/startup/*
