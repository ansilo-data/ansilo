FROM ansilo-source as source

# Run tests
RUN source $HOME/.cargo/env && cargo test
# Run benches
RUN source $HOME/.cargo/env && cargo bench
