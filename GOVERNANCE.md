# Orbit SQL PostgreSQL governance

This repository is developed in public as an independently installable Orbit package. Governance describes project practice; it does not replace hosting-platform permissions, branch rules, CI results, or release evidence.

## Participation

Anyone may open an issue, propose a design, submit a pull request, review documentation, or join public technical discussion. Contributions follow [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md). Vulnerability reports follow [SECURITY.md](SECURITY.md) and must not be filed publicly.

## Maintainers

The interim Orbit maintainers listed in [MAINTAINERS.md](MAINTAINERS.md) are responsible for the package contract, dependency direction, review, and release decisions. The [CODEOWNERS](.github/CODEOWNERS) file records the current review-request team; it does not grant repository access or change branch protection.

## Technical decisions

Routine changes are decided through reviewed pull requests. Record a design decision in a short ADR under `docs/architecture/adr/` when the package introduces or changes a public contract, dependency direction, lifecycle owner, security policy, supported-runtime range, or compatibility/deprecation policy. Backward-incompatible changes must document impact and migration guidance.

## Releases and accountability

Only authorized maintainers may publish package releases or alter repository settings. Stable releases require the documented local and hosted validation, artifact integrity, dependency and security review, and provenance evidence. Local green checks alone do not constitute a stable release. The [roadmap](ROADMAP.md) records direction without making dated delivery promises.

## Amendments

Change this policy through a reviewed pull request. Changes to package boundaries, security responsibilities, or release obligations should include an ADR and maintainer review.
