# Release Oxide

Merge a release PR when you are ready to ship. Release Please maintains that PR from Conventional Commits, updating `Cargo.toml`, the root package in `Cargo.lock`, `.release-please-manifest.json`, and `CHANGELOG.md`. The first automated release is explicitly bootstrapped to **0.2.0**. After that, versions are inferred from commits; breaking changes below 1.0 bump the minor version.

## Prepare changes

Use a Conventional Commit PR title and squash merge, for example `fix: preserve array order`, `feat: add an operator`, or `feat!: change the storage format`. CI checks the PR title. Release Please supports a `Release-As: x.y.z` commit footer when an explicit version override is needed.

The bot uses the repository's existing `RELEASE_TOKEN` Actions secret to create and update PRs so their CI workflows run. A missing, expired, or insufficiently scoped token causes the bot job to fail; it never silently falls back to `GITHUB_TOKEN`. The repository must allow Actions to create pull requests. A dedicated GitHub App token can replace this credential later.

Release Please only creates PRs; `skip-github-release` prevents it from creating tags or publishing releases. Release Drafter has been removed so there is one source of version and changelog changes.

## Publish

1. Review the bot's version and changelog PR and wait for all required checks.
2. Merge it. The **Release PR** workflow recognizes the merged, same-repository PR by its `autorelease: pending` label and exact merge commit. Ordinary PR merges do not publish.
3. Automation calls the release pipeline directly on that commit. It runs the full Rust and differential/shell suites, platform shell tests, dependency audit, workflow lint, and release-policy regressions.
4. It builds native Linux/macOS/Windows archives, extracts and exercises each package's CLI and embedded shell, and runs all required upstream tests against the packaged Linux executable.
5. It verifies one archive per platform, checksums, package contents, and that Linux is byte-for-byte the candidate recorded in the successful upstream report.
6. Under a per-tag publication lock, it checks the remote tag and draft state, creates the tag if needed, uploads only the verified assets, and publishes with an explicit stable/prerelease flag. A published release is never replaced. The merged PR is then marked `autorelease: tagged`.

The release pipeline is called directly because tags created with `GITHUB_TOKEN` do not trigger another push workflow. Manual tag pushes remain supported as a fallback, but are unnecessary for the normal release-PR flow.

The 0.2.0 release notes include the committed migration addendum in `docs/release-notes/0.2.0.md`. Version-two storage requires exporting with the old binary and importing into a separate database; existing collections are never relabeled or rewritten automatically.

## Validate without publishing

From a branch containing a matching Cargo version:

```sh
gh workflow run release.yml --repo fcoury/oxide \
  --ref BRANCH --field tag=v0.2.0 --field publish=false
```

Use the actual package version (currently `v0.1.7` until the first release PR is merged). A dry run validates the bot credential, runs every release check, builds all three packages, verifies the tested artifact, and skips tag creation and publication. Inspect the `upstream-report` and `verified-release` artifacts. The publisher's draft/prerelease/failure paths are covered by mocked regression tests; a dry run alone cannot exercise writes to GitHub.

```sh
python3 -m unittest discover -s tests/automation -p 'test_*.py' -v
```

The release tooling requires Python 3.11 or newer; workflows install Python 3.12. The separate upstream runner continues to support Python 3.9.

## Recover a failed release

Rerun the failed workflow on its original commit. Never move a release tag. A conflicting remote tag, already published release, unexpected draft asset, failed check, or mismatching package/report stops publication. Remove an unexpected draft asset deliberately before retrying; automation does not delete unknown assets for you. A tag or draft may remain if publishing fails after validation, but the published release will not be overwritten by a retry.

If publication succeeded but updating the PR label failed, verify the release and its assets, then replace `autorelease: pending` with `autorelease: tagged` on the merged release PR. Repository administrators can publish manually outside these workflows, so these guarantees apply to this automation rather than every possible GitHub action.
