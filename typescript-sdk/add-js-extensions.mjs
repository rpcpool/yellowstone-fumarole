import { fileURLToPath } from 'url';
import { dirname } from 'path';
import fs from 'fs';
import path from 'path';
import recast from 'recast';
import babelParser from '@babel/parser';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// List of external packages that require '.js' extensions
const packagesRequiringJsExtension = [
  'protobufjs/minimal',
  // Add other package paths as needed
];

function shouldAppendJsExtension(source) {
  // Check if the path has an extension already
  if (path.extname(source)) {
    return false;
  }

  // Check if the path is relative
  if (source.startsWith('./') || source.startsWith('../')) {
    return true;
  }

  // Check if the path is in the whitelist of external packages
  return packagesRequiringJsExtension.some(pkg => source === pkg || source.startsWith(`${pkg}/`));
}

function processFile(filePath) {
  const code = fs.readFileSync(filePath, 'utf8');
  const ast = recast.parse(code, {
    parser: {
      parse: (source) => babelParser.parse(source, {
        sourceType: 'module',
        plugins: ['typescript']
      })
    }
  });

  let modified = false;

  recast.types.visit(ast, {
    visitImportDeclaration(pathNode) {
      const source = pathNode.node.source.value;
      if (shouldAppendJsExtension(source)) {
        pathNode.node.source.value = `${source}.js`;
        modified = true;
      }
      return false;
    },
    visitExportNamedDeclaration(pathNode) {
      if (pathNode.node.source?.value) {
        const source = pathNode.node.source.value;
        if (shouldAppendJsExtension(source)) {
          pathNode.node.source.value = `${source}.js`;
          modified = true;
        }
      }
      return false;
    },
    visitExportAllDeclaration(pathNode) {
      if (pathNode.node.source?.value) {
        const source = pathNode.node.source.value;
        if (shouldAppendJsExtension(source)) {
          pathNode.node.source.value = `${source}.js`;
          modified = true;
        }
      }
      return false;
    }
  });

  if (modified) {
    const output = recast.print(ast).code;
    fs.writeFileSync(filePath, output, 'utf8');
    console.log(`Updated import/export paths in: ${filePath}`);
  }
}

function traverseDir(dir) {
  fs.readdirSync(dir).forEach((file) => {
    const fullPath = path.join(dir, file);
    const stat = fs.statSync(fullPath);

    if (stat.isDirectory()) {
      traverseDir(fullPath);
    } else if (stat.isFile() && path.extname(fullPath) === '.js') {
      processFile(fullPath);
    }
  });
}

function main() {
  const esmDir = path.resolve(__dirname, 'dist/esm');

  if (!fs.existsSync(esmDir)) {
    console.error(`Directory not found: ${esmDir}`);
    process.exit(1);
  }

  traverseDir(esmDir);
}

main();