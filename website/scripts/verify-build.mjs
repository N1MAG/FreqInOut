import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const siteRoot = path.resolve(".vitepress/dist");
const expectedHtmlPages = [
  "404.html",
  "index.html",
  "start-here.html",
  "install/index.html",
  "guide/first-radio.html",
  "integrations/mesh.html",
  "support.html",
];
const forbiddenText = [
  "docs/internal",
  "internal-testing",
  "private-testing",
  "production-release",
];
const htmlFiles = [];
const allFiles = [];
const failures = [];

function walk(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const fullPath = path.join(directory, entry.name);
    const relativePath = path.relative(siteRoot, fullPath);
    const stat = fs.lstatSync(fullPath);

    if (stat.isSymbolicLink()) {
      failures.push(`Generated artifact contains a symbolic link: ${relativePath}`);
      continue;
    }
    if (entry.isDirectory()) {
      walk(fullPath);
      continue;
    }
    allFiles.push(fullPath);
    if (entry.name.endsWith(".html")) htmlFiles.push(fullPath);
  }
}

if (!fs.existsSync(siteRoot)) {
  console.error(`Generated site does not exist: ${siteRoot}`);
  process.exit(1);
}

walk(siteRoot);

const actualHtmlPages = htmlFiles.map((file) => path.relative(siteRoot, file)).sort();
const expectedHtmlSet = new Set(expectedHtmlPages);
const actualHtmlSet = new Set(actualHtmlPages);

for (const expected of expectedHtmlPages) {
  if (!actualHtmlSet.has(expected)) failures.push(`Expected generated page is missing: ${expected}`);
}
for (const actual of actualHtmlPages) {
  if (!expectedHtmlSet.has(actual)) failures.push(`Unexpected generated HTML page: ${actual}`);
}

for (const file of allFiles) {
  if (!/\.(?:html|js|css|json|txt|xml)$/i.test(file)) continue;
  const source = fs.readFileSync(file, "utf8");
  for (const forbidden of forbiddenText) {
    if (source.includes(forbidden)) {
      failures.push(
        `${path.relative(siteRoot, file)} contains forbidden internal marker: ${forbidden}`,
      );
    }
  }
}

for (const file of htmlFiles) {
  const source = fs.readFileSync(file, "utf8");
  for (const match of source.matchAll(/(?:href|src)="([^"]+)"/g)) {
    const value = match[1];
    if (/^(?:https?:|mailto:|#|data:)/.test(value)) continue;

    let relativeTarget = value.replace(/^\/FreqInOut\/?/, "").split(/[?#]/)[0];
    if (!relativeTarget) relativeTarget = "index.html";

    const candidates = [
      path.join(siteRoot, relativeTarget),
      path.join(siteRoot, `${relativeTarget}.html`),
      path.join(siteRoot, relativeTarget, "index.html"),
    ];
    if (!candidates.some((candidate) => fs.existsSync(candidate))) {
      failures.push(`${path.relative(siteRoot, file)} has unresolved target: ${value}`);
    }
  }
}

if (failures.length) {
  console.error(failures.join("\n"));
  process.exit(1);
}

console.log(
  `Verified curated Pages artifact: ${htmlFiles.length} HTML pages, ${allFiles.length} total files.`,
);
