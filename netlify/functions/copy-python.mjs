import { cpSync, mkdirSync, rmSync } from 'fs'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'

const rootDir = dirname(fileURLToPath(import.meta.url))
const sourceDir = join(rootDir, 'python_backend')
const distDir = join(rootDir, 'dist')
const destDir = join(distDir, 'python_backend')

try {
  rmSync(destDir, { recursive: true, force: true })
} catch {}

mkdirSync(distDir, { recursive: true })
cpSync(sourceDir, destDir, { recursive: true })
