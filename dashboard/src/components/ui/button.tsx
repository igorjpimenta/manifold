import { forwardRef, type ButtonHTMLAttributes } from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "inline-flex items-center justify-center font-mono transition-colors disabled:opacity-50 disabled:pointer-events-none",
  {
    variants: {
      variant: {
        default: "bg-bg-elev2 text-fg border border-border rounded-md px-3 py-1.5 text-xs hover:bg-bg-elev",
        link:    "bg-transparent text-accent border-0 p-0 hover:underline",
        chip:    "bg-transparent text-fg-dim border border-border rounded-full px-[10px] py-[3px] text-[12px] hover:bg-bg-elev2",
      },
    },
    defaultVariants: { variant: "default" },
  }
);

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, ...props }, ref) => (
    <button ref={ref} className={cn(buttonVariants({ variant }), className)} {...props} />
  )
);
Button.displayName = "Button";

export { buttonVariants };
