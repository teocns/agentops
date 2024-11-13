#!/bin/bash
set -e

./start_all.sh
./novnc_startup.sh

pip install pdbpp

python -m pdb http_server.py > /tmp/server_logs.txt 2>&1 &


echo "✨ Computer Use Demo is ready!"
echo "➡️  Open http://localhost:8080 in your browser to begin"

STREAMLIT_SERVER_PORT=8501 python -m streamlit run computer_use_demo/streamlit.py
# Keep the container running
# tail -f /dev/null
