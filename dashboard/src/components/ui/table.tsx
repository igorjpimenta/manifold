import { forwardRef, type HTMLAttributes, type TdHTMLAttributes, type ThHTMLAttributes } from "react";
import { cn } from "@/lib/utils";

// Wrapped in an overflow-x container so tables that exceed the viewport on
// narrow screens become horizontally scrollable instead of blowing out the
// parent layout. The negative-margin/positive-padding trick lets the scroll
// area bleed past the Card's inner padding so the table edge meets the card
// edge on mobile (where every horizontal pixel matters).
export const Table = forwardRef<HTMLTableElement, HTMLAttributes<HTMLTableElement>>(
  ({ className, ...props }, ref) => (
    <div className="-mx-3 sm:-mx-4 px-3 sm:px-4 overflow-x-auto">
      <table
        ref={ref}
        className={cn("min-w-full border-collapse text-[13px] whitespace-nowrap", className)}
        {...props}
      />
    </div>
  )
);
Table.displayName = "Table";

export const THead = forwardRef<HTMLTableSectionElement, HTMLAttributes<HTMLTableSectionElement>>(
  ({ className, ...props }, ref) => <thead ref={ref} className={cn("", className)} {...props} />
);
THead.displayName = "THead";

export const TBody = forwardRef<HTMLTableSectionElement, HTMLAttributes<HTMLTableSectionElement>>(
  ({ className, ...props }, ref) => <tbody ref={ref} className={cn("", className)} {...props} />
);
TBody.displayName = "TBody";

export const TR = forwardRef<HTMLTableRowElement, HTMLAttributes<HTMLTableRowElement>>(
  ({ className, ...props }, ref) => (
    <tr ref={ref} className={cn("[&:not(:last-child)]:border-b border-border", className)} {...props} />
  )
);
TR.displayName = "TR";

export const TH = forwardRef<HTMLTableCellElement, ThHTMLAttributes<HTMLTableCellElement>>(
  ({ className, ...props }, ref) => (
    <th
      ref={ref}
      className={cn(
        "text-left px-[10px] py-[6px] text-fg-dim font-normal uppercase tracking-[0.4px] text-[11px] border-b border-border",
        className
      )}
      {...props}
    />
  )
);
TH.displayName = "TH";

export const TD = forwardRef<HTMLTableCellElement, TdHTMLAttributes<HTMLTableCellElement>>(
  ({ className, ...props }, ref) => (
    <td
      ref={ref}
      className={cn("text-left px-[10px] py-[6px]", className)}
      {...props}
    />
  )
);
TD.displayName = "TD";
