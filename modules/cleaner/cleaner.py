import os
import json
import re
import requests
from tqdm import tqdm
from PIL import Image
from io import BytesIO
import imagehash
import logging

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # 获取当前文件所在的目录
INPUT_PATH = os.path.join(BASE_DIR,'data/scraped/pairs.jsonl')
OUTPUT_PATH = os.path.join(BASE_DIR,'data/cleaned/cleaned_pairs.jsonl')
BAD_KEYWORDS = ['广告', '促销', '点击', '购买', '赞助', '点我', '扫码']

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def clean_text(text):
    text = re.sub(r'<.*?>', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s\u4e00-\u9fff.,!?]', '', text)
    return text.strip().lower()

def is_valid_text(text):
    if len(text) < 5:
        return False
    if any(word in text for word in BAD_KEYWORDS):
        return False
    return True

def get_image_hash_and_check_accessible(url):
    try:
        with requests.get(url, timeout=10, stream=True) as res:
            if res.status_code != 200:
                return None
            content = res.content
            img = Image.open(BytesIO(content))
            img = img.convert('RGB')
            img.thumbnail((256,256))
            if img.width < 100 or img.height < 100:
                return None
            return str(imagehash.phash(img))
    except Exception as e:
        logging.debug(f"获取图片失败: {url}, 错误: {e}")
        return None

def run_cleaning():
    if not os.path.exists(INPUT_PATH):
        logging.error('未找到原始图文对数据')
        return {'text_message': '未找到原始图文对数据', 'image_message': '跳过'}

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    cleaned = []
    seen_texts = set()
    seen_images = set()

    skipped_count = 0

    with open(INPUT_PATH, 'r', encoding='utf-8') as fin:
        for line in tqdm(fin, desc='清洗文本与图像验证'):
            try:
                item = json.loads(line)
                raw_text = item.get('text', '')
                image_url = item.get('image_url', '')
                if not raw_text or not image_url:
                    skipped_count += 1
                    continue

                text = clean_text(raw_text)
                if not is_valid_text(text) or text in seen_texts:
                    skipped_count += 1
                    continue

                img_hash = get_image_hash_and_check_accessible(image_url)
                if not img_hash or img_hash in seen_images:
                    skipped_count += 1
                    continue

                seen_texts.add(text)
                seen_images.add(img_hash)

                cleaned.append({'text': text, 'image_url': image_url})

            except json.JSONDecodeError as e:
                logging.warning(f"JSON 解码错误: {e}")
            except Exception as e:
                logging.warning(f"跳过一条数据，原因: {e}")

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as fout:
        for item in cleaned:
            fout.write(json.dumps(item, ensure_ascii=False) + '\n')

    logging.info(f"清洗完成，有效条目: {len(cleaned)}, 跳过条目: {skipped_count}")

    return {
        'text_message': f'有效清洗后图文对数量：{len(cleaned)}',
        'image_message': '图像链接有效性 + 图文去重完成'
    }


