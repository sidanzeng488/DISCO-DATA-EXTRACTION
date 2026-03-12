"""
DISCODATA Web Application
Simple Flask app for querying EEA DISCODATA
"""

from flask import Flask, render_template, request, jsonify, Response
import json
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.discodata_client import create_client

app = Flask(__name__)
client = create_client()

# Load config
def load_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'databases.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

@app.route('/')
def index():
    config = load_config()
    return render_template('index.html', tables=config['tables'])

@app.route('/api/columns/<table_id>')
def get_columns(table_id):
    """Get columns for a table"""
    config = load_config()
    
    # Find table
    table = next((t for t in config['tables'] if t['id'] == table_id), None)
    if not table:
        return jsonify({'error': 'Table not found'}), 404
    
    try:
        columns = client.get_columns(table['full_name'])
        return jsonify({'columns': columns, 'table': table})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/preview/<table_id>')
def preview_data(table_id):
    """Preview table data"""
    config = load_config()
    
    table = next((t for t in config['tables'] if t['id'] == table_id), None)
    if not table:
        return jsonify({'error': 'Table not found'}), 404
    
    try:
        data = client.preview(table['full_name'], rows=10)
        return jsonify({'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/query', methods=['POST'])
def query_data():
    """Query data with selected columns"""
    params = request.json
    config = load_config()
    
    table_id = params.get('table_id')
    columns = params.get('columns', [])
    where = params.get('where', '')
    limit = params.get('limit', 1000)
    
    table = next((t for t in config['tables'] if t['id'] == table_id), None)
    if not table:
        return jsonify({'error': 'Table not found'}), 404
    
    try:
        data = client.select(
            table['full_name'],
            columns=columns if columns else None,
            where=where if where else None,
            limit=int(limit) if limit else None
        )
        return jsonify({'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download', methods=['POST'])
def download_data():
    """Download data as CSV"""
    params = request.json
    config = load_config()
    
    table_id = params.get('table_id')
    columns = params.get('columns', [])
    where = params.get('where', '')
    limit = params.get('limit')
    
    table = next((t for t in config['tables'] if t['id'] == table_id), None)
    if not table:
        return jsonify({'error': 'Table not found'}), 404
    
    try:
        data = client.select(
            table['full_name'],
            columns=columns if columns else None,
            where=where if where else None,
            limit=int(limit) if limit else None
        )
        
        if not data:
            return jsonify({'error': 'No data returned'}), 404
        
        # Generate CSV
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        
        return Response(
            '\ufeff' + output.getvalue(),  # BOM for Excel
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={table_id}_export.csv'}
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000, load_dotenv=False)
