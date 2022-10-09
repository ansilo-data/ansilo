---
sidebar_position: 5
---

# Vault Integration

You can retrieve secrets from [HashiCorp Vault](https://www.vaultproject.io/) using directives in your `ansilo.yml`.

### Step 1: Add vault configuration

Add the vault configuration to your `ansilo.yml`:

```yaml
vault:
  # URL to vault instance
  address: https://your.vault.host
  # API Version
  # Default: 1
  version: 1
  # Namespace
  # Optional
  namespace: example_namespace
  # Performs TLS Verification. Unsafe to disable.
  # Default: true
  verify: true
  # Connection timeout in seconds
  # Default: none
  timeout_secs: 30
  # Authentication options
  auth:
    # Authentication type
    # One of "token", "userpass', "approle", "kubernetes"
    type: token
    # Vault token
    token: YOUR-VAULT-TOKEN
```

#### `userpass` auth

```yaml
vault:
  auth:
    type: userpass
    mount: mnt
    username: example_user
    password: example_pass
```

#### `approle` auth

```yaml
vault:
  auth:
    type: approle
    mount: mnt
    role_id: example_role_id
    secret_id: example_role_id
```

#### `kubernetes` auth

```yaml
vault:
  auth:
    type: kubernetes
    mount: mnt
    role: example_role
    jwt: jwt_goes_here
```

### Step 2: Define secrets to retrieve

| Directive                       | Replacement                                                                  |
| ------------------------------- | ---------------------------------------------------------------------------- |
| `${vault:mnt:/secret/path:key}` | Retrieves the secret from mount `mnt` at path `/secret/path` with key `key`. |
