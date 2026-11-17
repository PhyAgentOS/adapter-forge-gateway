#!/usr/bin/env bash
# 在项目自己的隔离环境中构建 Gateway 可执行文件。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_ENV="${ROOT}/.venv_build"
DIST_DIR="${ROOT}/dist"
WORK_DIR="${ROOT}/build/pyinstaller"
SPEC_FILE="${SCRIPT_DIR}/gateway.spec"
DIST_FILE="${DIST_DIR}/gateway"

cleanup() {
  rm -rf "${BUILD_ENV}"
}
trap cleanup EXIT

cd "${ROOT}"
rm -rf "${BUILD_ENV}" "${DIST_DIR}" "${WORK_DIR}"

echo "==> [gateway] 同步隔离构建环境..."
UV_PROJECT_ENVIRONMENT=.venv_build uv sync \
  --project "${ROOT}" \
  --frozen \
  --no-dev \
  --group build

echo "==> [gateway] 使用 PyInstaller 构建..."
"${BUILD_ENV}/bin/pyinstaller" \
  --noconfirm \
  --clean \
  --distpath "${DIST_DIR}" \
  --workpath "${WORK_DIR}" \
  "${SPEC_FILE}"

if [[ ! -x "${DIST_FILE}" ]]; then
  echo "ERROR: 未找到可执行产物 ${DIST_FILE}" >&2
  exit 1
fi

echo "==> [gateway] 验证可执行文件帮助信息..."
"${DIST_FILE}" --help

echo "==> [gateway] 验证可执行文件版本..."
VERSION_OUTPUT="$("${DIST_FILE}" --version)"
if [[ "${VERSION_OUTPUT}" != "forge-gateway 1.1.2" ]]; then
  echo "ERROR: 非预期版本输出：${VERSION_OUTPUT}" >&2
  exit 1
fi
printf '%s\n' "${VERSION_OUTPUT}"

echo "==> [gateway] 构建成功：${DIST_FILE}"
