# oc-pipeclaw

LangGraph-style pipeline runtime for openclaw. Plugin id: `pipeclaw`.
Private repository — not open-source.

## Install (live gateway)

    openclaw plugins install git:github.com/igorjpimenta/oc-pipeclaw@v<semver> --force

Managed git install; the installer places it at `~/.openclaw/extensions/pipeclaw`.
Bump the tag to update, then reload the plugin.

## Develop

- One story per PR; worktrees at `.doppler/worktrees/<story>`.
- `npm ci && npm run test:unit` (suites run under `scripts/with-guard.sh`).
- Angular commit convention; comment-policy scan before every PR.

Authoring docs (engine internals, subflow guide, node reference) arrive with
the docs story; the canonical copy meanwhile lives in `igorjpimenta/oc-config`
under `extensions/pipeclaw/`.
