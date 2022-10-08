---
sidebar_position: 7
---

# Troubleshooting

The container will output all logging to stdout/stderr, the logging output will be your first
diagnostic for any errors or issues.

The `RUST_LOG` environment variable controls the verbosity of the logging. If the default level
is not verbose enough it can be increased.

The [boilerplate repo](https://github.com/ansilo-data/template/) shows a provides a working development
environment where logging settings can be altered easily.

The following examples show possible logging options:

| `RUST_LOG`     | Description                                                                                                |
| -------------- | ---------------------------------------------------------------------------------------------------------- |
| `ansilo=info`  | High-level logging of key system operations, including warnings and errors. This is the default.           |
| `ansilo=debug` | Debug-level logging that includes additional diagnostic information.                                       |
| `ansilo=trace` | Shows all logging.                                                                                        |
| `trace`        | Shows all possible logging, including any that of any libraries used by Ansilo. This is the highest level. |
