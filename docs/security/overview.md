# Orbit SQL PostgreSQL: security

## Package-specific security guidance

The README does not define separate package-specific security guarantees. Follow the implemented API and provider documentation; do not infer security properties that are not documented and tested.

## Credentials and sensitive data

Use least-privilege credentials and a secret manager for provider secrets. Do not commit secrets, tokens, private keys, personal data, request bodies, raw provider errors, or opaque cursors. Enable TLS certificate verification for remote services unless package documentation gives a narrowly scoped local-development exception.

## Vulnerability disclosure

Follow the repository [security policy](../../SECURITY.md). Do not post vulnerability details in public issues. The policy describes private reporting alternatives without claiming that a hosting-platform setting is enabled.

## Limits

This page records source and package documentation, not an independent security audit. Validate the package, its provider SDKs, and its deployment configuration before production use.
