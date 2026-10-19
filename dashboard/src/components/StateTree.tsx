// Collapsible state-inspector. Recursive <details> trees so cytoscape's
// "click a node, scroll the matching state slot into view" affordance still
// works via [data-state-key] selectors.

import { useState } from "react";
import { cn } from "@/lib/utils";

export function StateTree({ value, label, defaultOpen = false }: { value: unknown; label: string; defaultOpen?: boolean }) {
  return <TreeNode value={value} label={label} defaultOpen={defaultOpen} />;
}

function TreeNode({ value, label, defaultOpen = false }: { value: unknown; label: string; defaultOpen?: boolean }) {
  const [open, setOpen] = useState<boolean>(defaultOpen);

  if (value === null || value === undefined) {
    return <div className="font-mono text-fg-dim text-xs"><span>{label}:</span> null</div>;
  }
  if (Array.isArray(value)) {
    return (
      <details
        open={open}
        data-state-key={label}
        // React onToggle bubbles up the synthetic-event tree, so a nested
        // <details> closing triggers this listener too. Ignore events that
        // didn't originate on this element; otherwise the parent collapses
        // whenever any descendant toggles.
        onToggle={(e) => {
          if (e.target !== e.currentTarget) return;
          setOpen((e.currentTarget as HTMLDetailsElement).open);
        }}
        className="my-0.5"
      >
        <summary className="cursor-pointer select-none text-fg-dim font-mono text-xs hover:text-fg">
          <span>{label}</span> <span className="text-fg-dim">[{value.length}]</span>
        </summary>
        {open && (
          <div className="ml-[14px] border-l border-border pl-2">
            {value.slice(0, 50).map((v, i) => (
              <TreeNode key={i} value={v} label={String(i)} />
            ))}
            {value.length > 50 && <div className="text-fg-dim font-mono">… {value.length - 50} more</div>}
          </div>
        )}
      </details>
    );
  }
  if (typeof value === "object") {
    const keys = Object.keys(value as object);
    return (
      <details
        open={open}
        data-state-key={label}
        // React onToggle bubbles up the synthetic-event tree, so a nested
        // <details> closing triggers this listener too. Ignore events that
        // didn't originate on this element; otherwise the parent collapses
        // whenever any descendant toggles.
        onToggle={(e) => {
          if (e.target !== e.currentTarget) return;
          setOpen((e.currentTarget as HTMLDetailsElement).open);
        }}
        className="my-0.5"
      >
        <summary className="cursor-pointer select-none text-fg-dim font-mono text-xs hover:text-fg">
          <span>{label}</span> <span className="text-fg-dim">{`{${keys.length}}`}</span>
        </summary>
        {open && (
          <div className="ml-[14px] border-l border-border pl-2">
            {keys.map((k) => (
              <TreeNode key={k} value={(value as Record<string, unknown>)[k]} label={k} />
            ))}
          </div>
        )}
      </details>
    );
  }
  // Primitive
  const text = typeof value === "string" ? `"${value}"` : String(value);
  const short = text.length > 200 ? text.slice(0, 197) + "…" : text;
  return (
    <div className="font-mono text-xs" title={text}>
      <span className="text-fg-dim">{label}:</span> <span className={cn(typeof value === "string" ? "text-fg" : "text-fg")}>{short}</span>
    </div>
  );
}
