from flask import Flask, request, jsonify, render_template_string, send_from_directory
from werkzeug.utils import secure_filename
from pathlib import Path
import os
import sys
import json
import logging

# Ensure the directory where utility.py is located is in the path
# This assumes utility.py is in the same directory as app.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the orchestrator's run function
try:
    from utility import run_orchestrator 
except ImportError as e:
    logging.error(f"Failed to import run_orchestrator from utility.py: {e}")
    sys.exit(1)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'output'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB limit

Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True)
Path(app.config['OUTPUT_FOLDER']).mkdir(exist_ok=True)

# Set up logging for console output visibility
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# --- Routes ---

@app.route('/')
def index():
    """Serves the main HTML page."""
    # Read the index.html file content (assuming it's in the same directory)
    try:
        with open('index.html', 'r') as f:
            html_content = f.read()
        return render_template_string(html_content)
    except FileNotFoundError:
        return "Error: index.html not found.", 500

@app.route('/api/migrate', methods=['POST'])
def migrate():
    """Handles file upload and runs the Python migration orchestrator."""
    if 'twb_files' not in request.files:
        return jsonify({'status': 'error', 'message': 'No files uploaded.'}), 400

    uploaded_files = request.files.getlist('twb_files')
    if not uploaded_files:
        return jsonify({'status': 'error', 'message': 'No TWB files selected.'}), 400

    # 1. Save uploaded files temporarily
    workbook_paths = []
    try:
        for file in uploaded_files:
            if file.filename.endswith('.twb'):
                filename = secure_filename(file.filename)
                file_path = Path(app.config['UPLOAD_FOLDER']) / filename
                file.save(file_path)
                workbook_paths.append(str(file_path))
            else:
                logging.warning(f"Skipping non-TWB file: {file.filename}")

        if not workbook_paths:
            return jsonify({'status': 'error', 'message': 'Only .twb files are supported.'}), 400

        # 2. Run the Orchestrator
        output_dir = Path(app.config['OUTPUT_FOLDER']) / 'migration_result'
        output_dir.mkdir(exist_ok=True, parents=True)
        
        # Clear previous run outputs
        for item in output_dir.glob('*'):
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                # Note: We won't recursively remove the directory to avoid permission issues
                # but in a production tool, you'd handle cleanup.
                pass 
        
        # This calls the run_orchestrator function we defined in utility.py
        result = run_orchestrator(workbook_paths, str(output_dir))
        
        # 3. Clean up uploaded files (optional, but good practice)
        for path in workbook_paths:
             os.remove(path)

        return jsonify({'status': 'success', 'result': result})

    except Exception as e:
        # Log the full exception for debugging
        logging.error(f"Migration failed during processing: {e}", exc_info=True)
        
        # Clean up any files left behind
        for path in workbook_paths:
            if os.path.exists(path):
                os.remove(path)
                
        return jsonify({'status': 'error', 'message': f'Processing failed: {e}'}), 500

# Route to serve generated files (e.g., the JSON report)
@app.route('/output/<path:filename>')
def serve_output(filename):
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename)

if __name__ == '__main__':
    logging.info("Starting Flask server...")
    app.run(debug=True, port=5000)

