#!/usr/bin/env python3
"""Minimal Flask dashboard for the log pipeline."""

from flask import Flask, render_template_string, jsonify
from hdfs import InsecureClient
import json
import os
from collections import defaultdict
import subprocess

app = Flask(__name__)

HDFS_URL = "http://spark-master:9870"   # namenode HTTP
HDFS_USER = os.environ.get("USER", "exouser")
ANOMALIES_PATH = "/logs/anomalies"

client = InsecureClient(HDFS_URL, user=HDFS_USER)


def read_recent_anomalies(limit_files=20):
    try:
        result = subprocess.run(
            ['hdfs', 'dfs', '-ls', '/logs/anomalies/'],
            capture_output=True, text=True
        )
        lines = [l for l in result.stdout.strip().split('\n') if 'part-' in l]
        # Sort by filename (newest last) and take the last N
        lines.sort()
        recent_files = [l.split()[-1] for l in lines[-limit_files:]]
        records = []
        for path in recent_files:
            cat = subprocess.run(
                ['hdfs', 'dfs', '-cat', path],
                capture_output=True, text=True
            )
            for line in cat.stdout.strip().split('\n'):
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except:
                        pass
        return records
    except Exception as e:
        return [{"error": str(e)}]


TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <title>Log Pipeline Dashboard</title>
  <meta http-equiv="refresh" content="10">
  <style>
    body { font-family: -apple-system, sans-serif; margin: 20px; background: #f5f5f5; }
    h1 { color: #1F3864; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
    .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 8px; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #D5E8F0; }
    .root { background: #FFE5E5; font-weight: bold; }
    .score-high { color: #c00; font-weight: bold; }
    .score-mid { color: #c80; }
    .score-low { color: #080; }
    .meta { color: #666; font-size: 0.9em; }
  </style>
</head>
<body>
  <h1>Distributed Log Pipeline — Live Dashboard</h1>
  <p class="meta">Auto-refresh every 10 seconds. Reading from HDFS /logs/anomalies.</p>

  <div class="grid">
    <div class="card">
      <h2>Top scored services (most recent windows)</h2>
      <table>
        <tr><th>Window</th><th>Service</th><th>Score</th><th>Persistence</th><th>Propagation</th><th>Magnitude</th><th>Multi-signal</th></tr>
        {% for r in top_scored %}
        <tr class="{% if r.is_root_cause_candidate %}root{% endif %}">
          <td>{{ r.window_start }}</td>
          <td>{{ r.service }}</td>
          <td class="{% if r.score > 0.6 %}score-high{% elif r.score > 0.3 %}score-mid{% else %}score-low{% endif %}">{{ "%.3f"|format(r.score) }}</td>
          <td>{{ "%.2f"|format(r.persistence) }}</td>
          <td>{{ "%.2f"|format(r.propagation) }}</td>
          <td>{{ "%.2f"|format(r.magnitude) }}</td>
          <td>{{ "%.2f"|format(r.multi_signal) }}</td>
        </tr>
        {% endfor %}
      </table>
    </div>

    <div class="card">
      <h2>Root cause candidates (last hour)</h2>
      <table>
        <tr><th>Window</th><th>Service</th><th>Score</th></tr>
        {% for r in root_candidates %}
        <tr class="root">
          <td>{{ r.window_start }}</td>
          <td>{{ r.service }}</td>
          <td class="score-high">{{ "%.3f"|format(r.score) }}</td>
        </tr>
        {% else %}
        <tr><td colspan="3" class="score-low">No root cause candidates detected. System is healthy.</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>

  <div class="card" style="margin-top: 20px;">
    <h2>Total records loaded</h2>
    <p>{{ total }} score records from {{ files }} HDFS files.</p>
  </div>
</body>
</html>
"""


@app.route("/")
def index():
    records = read_recent_anomalies(limit_files=20)
    valid = [r for r in records if "error" not in r]
    # Sort by window_start DESC first, then score DESC
    top_scored = sorted(valid,
        key=lambda x: (x.get("window_start", ""), x.get("score", 0)),
        reverse=True)[:30]
    root_candidates = [r for r in valid if r.get("is_root_cause_candidate")]
    root_candidates.sort(key=lambda x: x.get("window_start", ""), reverse=True)
    root_candidates = root_candidates[:20]
    return render_template_string(
        TEMPLATE,
        top_scored=top_scored,
        root_candidates=root_candidates,
        total=len(valid),
        files=20,
    )


@app.route("/api/anomalies")
def api_anomalies():
    return jsonify(read_recent_anomalies())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=False)
