// link-core-self.mjs — make `@igorjpimenta/manifold/<path>` resolvable in a
// repo checkout.
//
// The adapter imports core through the published package name, which resolves
// from node_modules when installed. In the repo the core is the root package,
// and npm cannot link the root into its own node_modules as a workspace, so
// the link is made here. Installed consumers never run this: the dependency
// resolves from the registry.
//
// Idempotent; a non-writable node_modules (host install) is skipped silently.

import { existsSync, lstatSync, mkdirSync, symlinkSync, rmSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const scopeDir = path.join(root, "node_modules", "@igorjpimenta");
const link = path.join(scopeDir, "manifold");

// Installed consumers have no adapter package beside them; nothing to link.
if (!existsSync(path.join(root, "plugins", "openclaw", "manifold"))) process.exit(0);

try {
  if (existsSync(link) && !lstatSync(link).isSymbolicLink()) process.exit(0);
  mkdirSync(scopeDir, { recursive: true });
  if (existsSync(link)) rmSync(link, { force: true });
  symlinkSync(root, link, "dir");
} catch {
  // Read-only or absent node_modules: the dependency resolves normally when
  // the package is installed for real.
}
