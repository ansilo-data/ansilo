FROM ansilo-source as source

ENV ANSILO_TEST_PG_DIR=/usr/pgsql-14/

# Run tests
RUN source $HOME/.cargo/env && cargo test
# Run benches
RUN source $HOME/.cargo/env && cargo bench
