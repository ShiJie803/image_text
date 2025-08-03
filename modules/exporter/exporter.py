import os
import json
import csv
import pandas as pd

INPUT_DIR = 'data/split'
OUTPUT_DIR = 'data/exported'

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
    try:
        with open(file_path, 'w', encoding='utf-8') as fout:
            for item in data:
                fout.write(json.dumps(item, ensure_ascii=False) + '\n')
    except Exception as e:
        raise RuntimeError(f"写入JSONL失败: {file_path}, 错误: {e}")

def export_csv(data, file_path):
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
    try:
        df = pd.DataFrame(data)
        df.to_parquet(file_path, index=False)
    except Exception as e:
        raise RuntimeError(f"写入Parquet失败: {file_path}, 错误: {e}")

def run_export(format='jsonl'):
    if format not in ['jsonl', 'csv', 'parquet']:
        return {'status': 'error', 'message': '不支持的导出格式'}

    if not os.path.exists(INPUT_DIR):
        return {'status': 'error', 'message': f'输入目录不存在: {INPUT_DIR}'}

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    datasets = ['train', 'val', 'test']
    for ds in datasets:
        input_file = os.path.join(INPUT_DIR, f'{ds}.jsonl')
        if not os.path.exists(input_file):
            return {'status': 'error', 'message': f'缺少数据集文件: {ds}.jsonl'}

    try:
        data_dict = {ds: read_jsonl(os.path.join(INPUT_DIR, f'{ds}.jsonl')) for ds in datasets}
        for ds in datasets:
            out_path = os.path.join(OUTPUT_DIR, f'{ds}.{format}')
            if format == 'jsonl':
                export_jsonl(data_dict[ds], out_path)
            elif format == 'csv':
                export_csv(data_dict[ds], out_path)
            elif format == 'parquet':
                export_parquet(data_dict[ds], out_path)
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

    return {'status': 'success', 'message': f'数据集已导出为 {format} 格式，路径：{OUTPUT_DIR}'}

