#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="p2p-node"        
SERVICE_FILE="${SERVICE_NAME}.service"
NODE_SYMLINK="/usr/bin/node"

install_node() {
  echo "Node.js is not installed – installing Node.js 22 LTS from NodeSource..."
  apt-get update
  apt-get install -y curl ca-certificates gnupg
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y nodejs make g++
}

ensure_node() {
  if command -v node >/dev/null 2>&1; then
    NODE_BIN=$(command -v node)
    echo "Found Node.js at $NODE_BIN"
  else
    install_node
    NODE_BIN=$(command -v node)
  fi

  if [ ! -e "$NODE_SYMLINK" ]; then
    echo "Creating symlink $NODE_SYMLINK → $NODE_BIN"
    ln -s "$NODE_BIN" "$NODE_SYMLINK"
  fi
}

install_dependencies() {
  echo "Installing project dependencies..."
  npm install --production
}

deploy_service() {
  echo "Copying $SERVICE_FILE to /etc/systemd/system/"
  cp "$SERVICE_FILE" /etc/systemd/system/

  echo "Reloading systemd and starting the service"
  systemctl daemon-reload
  systemctl enable --now "$SERVICE_NAME"
}

main() {
  if [ "$EUID" -ne 0 ]; then
    echo "Please run this script with sudo or as root." >&2
    exit 1
  fi

  ensure_node
  install_dependencies
  deploy_service

  echo "All done! Check status with: systemctl status $SERVICE_NAME"
}

main "$@"
