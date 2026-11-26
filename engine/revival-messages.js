// revival-messages.js — shared boot-revival continuation prompt. Sent to
// the EXISTING session key after a restart interrupts an in-flight agent
// turn (see executor.js's reviveAgentKindOrphan for why a fresh
// re-dispatch would risk a duplicate side effect).

export const REVIVAL_MSG =
  "Continue from where you left off — your prior session was interrupted by a gateway restart or process death. Read your conversation history, identify the in-flight task, and resume or finalize it.";
