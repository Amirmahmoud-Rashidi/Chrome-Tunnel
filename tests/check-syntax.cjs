const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const root = path.join(__dirname, '..');
function walk(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    if (['node_modules', '.git', 'ca'].includes(entry.name)) return [];
    const file = path.join(dir, entry.name);
    return entry.isDirectory() ? walk(file) : /\.(js|cjs)$/.test(file) ? [file] : [];
  });
}
let failed = false;
for (const file of walk(root)) {
  const result = spawnSync(process.execPath, ['--check', file], { encoding: 'utf8' });
  if (result.status !== 0) { failed = true; console.error(result.stderr); }
}
console.log(failed ? 'Syntax check failed' : 'All JavaScript syntax checks passed');
process.exit(failed ? 1 : 0);
