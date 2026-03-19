"""
Avvia un server locale per Il Mio Foglio Portfolio Builder.
Apre automaticamente il browser su http://localhost:8080
"""
import http.server
import webbrowser
import os
import threading

PORT = 8080
DIR = os.path.dirname(os.path.abspath(__file__))

os.chdir(DIR)

handler = http.server.SimpleHTTPRequestHandler
server = http.server.HTTPServer(('localhost', PORT), handler)

print(f"Server avviato: http://localhost:{PORT}")
print(f"Cartella: {DIR}")
print("Premi Ctrl+C per chiudere")

# Apri il browser dopo 0.5 secondi
threading.Timer(0.5, lambda: webbrowser.open(f'http://localhost:{PORT}/index.html')).start()

try:
    server.serve_forever()
except KeyboardInterrupt:
    print("\nServer chiuso.")
    server.server_close()
