// Explicit file discovery works on Windows without shell glob expansion.
const { readdirSync } = require('node:fs');
const path = require('node:path');
const { spawn } = require('node:child_process');
const mode = process.argv[2] || 'all';
if (!['all', 'core', 'regressions', 'coverage'].includes(mode)) {
  console.error('Usage: node tests/run.cjs [all|core|regressions|coverage]');
  process.exit(2);
}
const groups = mode === 'core' ? ['unit', 'integration'] :
  mode === 'regressions' ? ['regressions'] : ['unit', 'integration', 'regressions'];
const files = groups.flatMap(group => readdirSync(path.join(__dirname, group))
  .filter(file => file.endsWith('.test.cjs')).sort()
  .map(file => path.join(__dirname, group, file)));
const child = spawn(process.execPath, [
  '--test', '--test-concurrency=2', '--test-timeout=20000', '--test-reporter=tap',
  ...(mode === 'coverage' ? ['--experimental-test-coverage'] : []), ...files,
], { stdio: 'inherit', cwd: path.join(__dirname, '..') });
child.on('error', error => { console.error(error); process.exitCode = 1; });
// Let stdout drain naturally; an explicit exit can truncate redirected TAP.
child.on('close', code => { process.exitCode = code ?? 1; });
