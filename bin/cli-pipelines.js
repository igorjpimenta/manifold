// cli-pipelines.js — pipelines register/list subcommands.
// Deps keys used: pipelineDirsStore, resolvePipelinesDir.
import { PIPELINES_DIR, withExit } from "./cli.js";

export function registerPipelineCommands(root, deps) {
  const { pipelineDirsStore, resolvePipelinesDir } = deps;
  // Adapter-resolved pipelines dir for every CLI load below (table →
  // env → config → default); standalone one-shot use keeps PIPELINES_DIR.
  const cliPipelinesDir = (() => {
    try { return resolvePipelinesDir?.()?.dir ?? PIPELINES_DIR; }
    catch { return PIPELINES_DIR; }
  })();

  // manifold pipelines register --harness <h> --path <dir> — upsert a
  // per-harness override (dir must exist). Takes effect on next gateway
  // boot; `list` shows the currently effective dir.
  // manifold pipelines list [--harness <h>] — table rows + effective dir.
  const pipelinesCmd = root
    .command("pipelines")
    .description("Inspect or override per-harness pipeline directories.");
  pipelinesCmd
    .command("register")
    .description("Register a per-harness pipeline directory override (upsert; dir must exist).")
    .requiredOption("--harness <h>", "Harness this override applies to")
    .requiredOption("--path <dir>", "Pipeline directory (must exist)")
    .action(withExit(async (options) => {
      if (!pipelineDirsStore) {
        console.error("manifold: pipeline dir store unavailable in this process.");
        process.exit(1);
      }
      try {
        const reg = pipelineDirsStore.register({ harness: options.harness, dirPath: options.path });
        console.log(`manifold: registered pipelines dir for harness "${reg.harness}": ${reg.path}`);
      } catch (err) {
        console.error(`manifold: ${err?.message ?? err}`);
        process.exit(1);
      }
    }));
  pipelinesCmd
    .command("list")
    .description("List registered per-harness pipeline dirs and the effective dir.")
    .option("--harness <h>", "Harness to resolve the effective dir for")
    .action(withExit(async (options) => {
      if (!pipelineDirsStore) {
        console.error("manifold: pipeline dir store unavailable in this process.");
        process.exit(1);
      }
      const rows = pipelineDirsStore.list();
      if (rows.length === 0) {
        console.log("manifold: no per-harness pipeline dirs registered.");
      } else {
        for (const r of rows) console.log(`  ${r.harness}  ${r.path}`);
      }
      const h = options.harness ?? null;
      const resolved = resolvePipelinesDir ? resolvePipelinesDir(h) : { dir: cliPipelinesDir, source: "default" };
      console.log(`manifold: effective dir${h ? ` for harness "${h}"` : ""}: ${resolved.dir} (source: ${resolved.source})`);
    }));
}
