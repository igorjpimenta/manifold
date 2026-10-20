// Fires when a mousedown/touchstart lands outside the given element.
// Used by chart components to unpin the focused day when the operator
// taps anywhere else on the page. mousedown is preferred over click so
// the unpin happens before the next bar's own click handler resolves
// — avoids a flicker where pin → unpin → re-pin races on the same tap.

import { useEffect, type RefObject } from "react";

export function useClickOutside<T extends HTMLElement>(
  ref: RefObject<T | null>,
  onOutside: () => void,
) {
  useEffect(() => {
    function handle(event: MouseEvent | TouchEvent) {
      const node = ref.current;
      const target = event.target;
      if (!node || !(target instanceof Node)) return;
      if (node.contains(target)) return;
      onOutside();
    }
    document.addEventListener("mousedown", handle);
    document.addEventListener("touchstart", handle);
    return () => {
      document.removeEventListener("mousedown", handle);
      document.removeEventListener("touchstart", handle);
    };
  }, [ref, onOutside]);
}
