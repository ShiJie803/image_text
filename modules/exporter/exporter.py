import os
import json
import csv
import pandas as pd

# 项目根目录（../../ 相对路径跳出模块文件夹）
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

INPUT_FILE = os.path.join(BASE_DIR, 'data', 'cleaned', 'cleaned_pairs.jsonl')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'exported')


def read_jsonl(file_path):
    data = []
    try:
        with open(file_path, 'r', encoding='utf-8') as fin:
            for line in fin:
                data.append(json.loads(line))
    except Exception as e:
        raise RuntimeError(f"读取文件失败: {file_path}, 错误: {e}")
    return data


def export_jsonl(data, file_path):
    # 删除旧文件，避免覆盖异常
    if os.path.exists(file_path):
        os.remove(file_path)

    try:
        with open(file_path, 'w', encoding='utf-8') as fout:
            for item in data:
                fout.write(json.dumps(item, ensure_ascii=False) + '\n')
    except Exception as e:
        raise RuntimeError(f"写入JSONL失败: {file_path}, 错误: {e}")


def export_csv(data, file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

    keys = ['image_url', 'text']
    try:
        with open(file_path, 'w', encoding='utf-8', newline='') as fout:
            writer = csv.DictWriter(fout, fieldnames=keys)
            writer.writeheader()
            for item in data:
                writer.writerow({k: item.get(k, '') for k in keys})
    except Exception as e:
        raise RuntimeError(f"写入CSV失败: {file_path}, 错误: {e}")


def export_parquet(data, file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

    try:
        df = pd.DataFrame(data)
        df.to_parquet(file_path, index=False)
    except Exception as e:
        raise RuntimeError(f"写入Parquet失败: {file_path}, 错误: {e}")


def run_export(format='jsonl'):
    format = format.lower()
    if format not in ['jsonl', 'csv', 'parquet']:
        return {'status': 'error', 'message': '不支持的导出格式'}

    if not os.path.exists(INPUT_FILE):
        return {'status': 'error', 'message': f'输入文件不存在: {INPUT_FILE}'}

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        data = read_jsonl(INPUT_FILE)
        output_path = os.path.join(OUTPUT_DIR, f'cleaned_pairs.{format}')

        if format == 'jsonl':
            export_jsonl(data, output_path)
        elif format == 'csv':
            export_csv(data, output_path)
        elif format == 'parquet':
            export_parquet(data, output_path)

    except Exception as e:
        return {'status': 'error', 'message': str(e)}

    return {
        'status': 'success',
        'message': f'数据集已导出为 {format} 格式，路径：{output_path}',
        'path': output_path  # 可用于页面下载或文件定位
    }
