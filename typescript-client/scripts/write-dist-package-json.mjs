// tsc emits plain `.js` files into both dist/cjs and dist/esm — the file
// extension alone doesn't tell Node which module system to use. The root
// package.json's `"type": "module"` would make Node parse dist/cjs's
// `require`/`exports` output as ESM and fail, so each dist subtree gets its
// own package.json pinning its module type, overriding the root for anything
// under it. See: https://nodejs.org/api/packages.html#dual-commonjses-module-packages
import { writeFileSync } from 'node:fs'

writeFileSync('dist/cjs/package.json', JSON.stringify({ type: 'commonjs' }) + '\n')
writeFileSync('dist/esm/package.json', JSON.stringify({ type: 'module' }) + '\n')
