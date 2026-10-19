import { cva, type VariantProps } from "class-variance-authority";
import type { HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

// Status variants match the run-status palette established in v0
// (succeeded → blue tint, failed → red, etc.). New variants `pill-*` are
// for the path-trail / per-step pills that appear inside tables.
const badgeVariants = cva(
  "inline-block px-2 py-[2px] rounded-[10px] font-mono text-[11px] border border-transparent leading-snug whitespace-nowrap",
  {
    variants: {
      variant: {
        default:    "bg-bg-elev2 text-fg-dim border-border",
        running:    "bg-status-green/15  text-status-green  border-status-green/40",
        waiting:    "bg-status-amber/15  text-status-amber  border-status-amber/40",
        blocked:    "bg-status-amber/15  text-status-amber  border-status-amber/40",
        succeeded:  "bg-status-blue/15   text-[#79b8ff]     border-status-blue/40",
        failed:     "bg-status-red/15    text-status-red    border-status-red/40",
        cancelled:  "bg-status-grey/15   text-status-grey   border-status-grey/40",
        kind:       "bg-bg-elev2 text-fg-dim border-border",
        historical: "bg-status-grey/15   text-fg-dim        border-border",
      },
      size: {
        default: "text-[11px] px-2",
        sm:      "text-[10px] px-[6px] py-px",
      },
    },
    defaultVariants: { variant: "default", size: "default" },
  }
);

export interface BadgeProps extends HTMLAttributes<HTMLSpanElement>, VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, size, ...props }: BadgeProps) {
  return <span className={cn(badgeVariants({ variant, size }), className)} {...props} />;
}

export { badgeVariants };
