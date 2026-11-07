# Releasing

CalVer, one tag per release, and the GitHub release is the changelog — there
is no CHANGELOG.md. The tag is the install contract: the live gateway and
every consumer pin resolve to it, so the version is decided in the release PR
before the tag is cut, never after.

This repo is private and `@igorjpimenta/manifold` is UNLICENSED. It is
never npm-published; a git tag is the only distribution channel.

## Version

`YYYY.M.D` in `package.json`, carrying the merge date in UTC — a 23:30
-0300 merge is already tomorrow, and the release workflow fails a tag
whose date is not UTC today. A release that takes
three weeks still gets the day it landed, because the tag is cut on the merge
commit and that is the version anyone installs.

The tag is the version, verbatim — no `v` prefix: `package.json`
`2026.10.20` → tag `2026.10.20`. Tags are never rewritten or deleted — a
consumer pin is permanent; a mistake ships as a new date, not a retag.
A same-day rebuild appends `-beta.N` (`2026.10.20-beta.1`); a fix that
ships tomorrow gets tomorrow's date. Numbering starts at `beta.1` —
never `beta.0`: the installer's strict CalVer parser requires the
sequence above zero and rejects `beta.0`, so a zero-suffixed tag is
uninstallable by contract.

## Order of operations

1. Merge every pending story PR into `main`. `main` receives code only
   through PRs; a tag push is not a branch push and never moves `main`.
2. Open ONE release PR: bump `package.json` `version`, commit subject
   `chore(release): <version>`. No code changes ride the release PR; docs
   keep the `@<version>` placeholder, the released tag is the record.
3. Merge the release PR.
4. On the merge commit: `git tag <version> -m "<version> — <summary>"`,
   then push the tag.
5. The tag push fires the release workflow (`.github/workflows/release.yml`),
   which verifies the tag matches `package.json` verbatim, rejects `beta.0`,
   runs the unit suite, and publishes the GitHub release with generated
   notes. Edit the notes afterward to the hand-written shape below. Manual
   fallback (workflow down or re-publish after a notes edit):
   `gh release create <version> --title <version> --notes <notes>`.

Publish at the moment the tag ships. A backfilled release makes the releases
page claim versions appeared on the day they were written up.

## Notes

Lower case, no trailing full stop, a full sentence where it reads like one —
`2026.10.20 — live refresh, so a policy edit needs no restart`. Notes: what
changed, how to upgrade (the consumer lines below), what it costs. Test
counts are omitted; a consumer cannot act on them.

## Consumers of a release

Live gateway (shorthand, pinned tag, capability consent — the installer
refuses a plugin whose capabilities the user has not accepted; the plugin
reaches `child_process` through `bin/cli.js`, and the host's unsafe-install
flag is a deprecated no-op):

    openclaw plugins install git:igorjpimenta/manifold@<version> --force --accept-capabilities

then reload the plugin. Managed installs land under `~/.openclaw/git/`
(resolve the exact path from `~/.openclaw/plugins/installs.json`,
`installPath` for id `manifold`), never under `extensions/`. Build the
dashboard there explicitly — managed installs pass `--ignore-scripts`, so
`prepare` never runs there and `dashboard/node_modules` is absent
(`prepare` only covers npm and git-dependency consumers):

    npm --prefix <installPath>/dashboard ci
    npm --prefix <installPath>/dashboard run build

Consumers bump the `#<version>` pins in their npm git-deps
(`workspace/*`, `extensions/*`) and regen their lockfiles — one chore PR
per release.

Consumer npm deps pin `git+https://github.com/igorjpimenta/manifold.git#<version>`.

Nothing follows `main` automatically — consumers bump the pin and re-install.

## compat

`openclaw.compat.openclawMin` in `package.json` is the host floor. Bump it
deliberately, only when the plugin starts requiring newer host APIs, and
call the raise out in the release notes — consumers pinning older tags stay
put.
