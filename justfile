# Backend (Port 3000) und Frontend (Port 5173) gleichzeitig starten; Strg+C beendet beide
dev:
    #!/usr/bin/env bash
    trap 'trap "" TERM; kill 0' EXIT
    (cd backend && MCP_TOKEN="${MCP_TOKEN:-dev}" cargo run --release) & # MCP für Agenten: http://127.0.0.1:3000/mcp/dev
    (cd frontend && npm run dev) &
    wait -n # endet einer der beiden, wird der andere über den trap mit beendet

# MaStR-Gesamtdatenexport nach backend/mastr.db importieren
import zip:
    cd backend && cargo run --release -- import "{{absolute_path(zip)}}"
