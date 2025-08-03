import os
from flask import Flask, render_template, request, jsonify
from modules.cleaner.cleaner import run_cleaning
from modules.crawler.crawler import run_crawler
from modules.exporter.exporter import run_export

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/startScraping', methods=['POST'])
def start_scraping():
    try:
        data = request.get_json(force=True)
        input_text = data.get('url', '').strip()
        threads = data.get('threads', 3)

        if not input_text:
            return jsonify({'status': 'error', 'message': '请输入关键词或网址'}), 400

        try:
            threads = int(threads)
            if threads < 1 or threads > 5:
                return jsonify({'status': 'error', 'message': '线程数应在1到20之间'}), 400
        except (ValueError, TypeError):
            return jsonify({'status': 'error', 'message': '线程数必须为整数'}), 400

        result_msg = run_crawler(start_input=input_text, threads=threads)
        return jsonify({'status': 'success', 'message': result_msg})

    except Exception as e:
        data = request.get_json(force=True)
        input_text = data.get('url', '').strip()
        print(f"[ERROR] startScraping with input '{input_text}': {e}")
        return jsonify({'status': 'error', 'message': f'服务器错误：{str(e)}'}), 500

@app.route('/startCleaning', methods=['POST'])
def start_cleaning():
    try:
        result = run_cleaning()
        return jsonify({'status': 'success', **result})
    except Exception as e:
        print(f"[ERROR] startCleaning: {e}")
        return jsonify({'status': 'error', 'text_message': '执行异常', 'image_message': str(e)}), 500

@app.route('/exportData', methods=['POST'])
def export_data():
    try:
        data = request.get_json(force=True)
        format_ = data.get('format', 'jsonl').lower()
        if format_ not in ['jsonl', 'csv', 'parquet']:
            return jsonify({'status': 'error', 'message': '不支持的导出格式'}), 400

        result = run_export(format_)
        return jsonify({'status': 'success', **result})
    except Exception as e:
        print(f"[ERROR] exportData: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
