// ios/node-lookup.js — pure lookup helpers over a loaded pipeline's node
// map, extracted from ios/index.js's nodeDiscussLookup closure so the
// subflow-nested-node resolution logic (the part that was buggy) can be
// unit-tested without spinning up the whole ios bridge / pipeline registry.

// `meta` is the { nodes } shape ios/index.js stores per pipeline in
// pipelinesById (pipeline.nodes as loaded by engine/loader.js).
export function resolveNodeDiscuss(meta, nodeId) {
  let node = meta?.nodes?.[nodeId];
  if (!node) {
    // nodeId may not be a top-level pipeline.nodes key — a human-gate like
    // GATE can live nested inside a subflow's flow.nodes (e.g.
    // PROPOSE.flow.nodes.GATE). Search subflow nodes' branches for a
    // matching sub-node, same as nodeAgentLookup in ios/index.js —
    // gate-projector.js's projectGateItem looks discuss up by the
    // actual awaiting node id, not just its parent subflow's id.
    for (const topNode of Object.values(meta?.nodes ?? {})) {
      if (topNode?.kind !== "subflow") continue;
      const subNode = topNode?.flow?.nodes?.[nodeId];
      if (subNode) { node = subNode; break; }
    }
    if (!node) return null;
  }
  let discussBlock = null;
  if (node.kind === "human-gate") {
    discussBlock = node.discuss;
  } else if (node.kind === "subflow") {
    for (const subNode of Object.values(node.flow?.nodes ?? {})) {
      if (subNode?.kind === "human-gate" && subNode?.discuss) { discussBlock = subNode.discuss; break; }
    }
  }
  if (!discussBlock || typeof discussBlock !== "object" || typeof discussBlock.agent !== "string") return null;
  return { agent: discussBlock.agent };
}
