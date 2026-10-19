// revival-messages.js — shared boot-revival continuation prompt.
//
// Sent to an EXISTING session key after a gateway restart interrupts an
// in-flight agent turn. Never re-dispatches the original task with a fresh
// prompt — that would give the agent no way to know it already tried this
// once, risking a duplicate side effect (e.g. a duplicate submission) if the
// prior attempt actually completed before the crash. Sending this to the
// same session lets the agent check its own history first.

export const REVIVAL_MSG =
  "Continue from where you left off — your prior session was interrupted by a gateway restart or process death. Read your conversation history, identify the in-flight task, and resume or finalize it.";
