/**
 * Radix UI Color System (Based on Radix UI Colors)
 *
 * Credit: The color palette and scales are based on the fantastic open source [Radix UI Colors](https://www.radix-ui.com/colors) project.
 *
 * Note: We do not use the official @radix-ui/colors packages directly because they are not directly compatible with Docusaurus' CSS system.
 * Docusaurus and Radix UI both provide their own set of CSS variables and themes, which can conflict, and Radix UI's packages are designed with their own tooling and component requirements in mind.
 * For maximum compatibility and to keep all variables available as global CSS custom properties, we copy the Radix colors as a CSS file and define them under the `:root` selector.
 * This ensures stable use of Radix-style scales across all Docusaurus pages and components.
 *
 * All colors are defined in colors.css and available as CSS custom properties.
 *
 * Usage in CSS:
 *   color: var(--radix-indigo-9);
 *   background-color: var(--radix-green-3);
 *   border-color: var(--radix-color-border);
 *
 * Usage in inline styles:
 *   style={{ color: 'var(--radix-indigo-9)' }}
 *
 * Available color scales:
 * - gray, indigo, cyan, orange, crimson, green, blue, purple, pink, red, yellow
 *
 * Each scale has 12 steps (1-12) following Radix UI convention:
 * - 1-2: Very light backgrounds
 * - 3-5: Subtle backgrounds
 * - 6-8: Borders and dividers
 * - 9-11: Text colors
 * - 12: Highest contrast text
 *
 * Semantic aliases:
 * - --radix-color-text
 * - --radix-color-text-subtle
 * - --radix-color-text-muted
 * - --radix-color-border
 * - --radix-color-border-subtle
 * - --radix-color-background
 * - --radix-color-background-subtle
 * - --radix-color-background-muted
 * - --radix-color-primary
 * - --radix-color-primary-hover
 * - --radix-color-primary-active
 */

// This file serves as documentation and entry point.
// The actual colors are defined in colors.css and imported globally in custom.css.

export {};
