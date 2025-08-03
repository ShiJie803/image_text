import uuid
import time
import requests
from io import BytesIO
from bs4 import BeautifulSoup
import re
import json
import os
from urllib.parse import urljoin
from tqdm import tqdm
import cloudinary
import cloudinary.uploader
from concurrent.futures import ThreadPoolExecutor, as_completed

# === 配置区域 ===
SEARCH_ENGINE = 'google'
MAX_IMAGES = 20
FILTER_DUPLICATES = False  # 简单按 image_url 去重，非深度图文相似度过滤

CLOUD_FOLDER = "paired_images"
OUTPUT_JSONL = "data/scraped/pairs.jsonl"  # 统一到 data/ 目录，方便后续清洗模块复用

# === Cloudinary 配置 ===
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET")
)

def fetch_and_extract(url):
    try:
        res = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://zh.wikipedia.org/"
            },
            timeout=20
        )
        if res.status_code != 200:
            return []

        soup = BeautifulSoup(res.text, 'html.parser')
        content_div = soup.find('div', id='mw-content-text') or soup

    except Exception as e:
        print(f"请求失败: {url}, 错误: {e}")
        return []

    data = []
    images = content_div.find_all('img')

    for img in images:
        src = (
            img.get('src') or
            img.get('data-src') or
            img.get('data-original') or
            img.get('srcset') or
            img.get('data-srcset')
        )
        if not src:
            continue
        img_url = urljoin(url, src)

        text = ''
        parent = img.parent
        sibling_texts = []

        for sibling in parent.children:
            if sibling == img:
                continue
            if isinstance(sibling, str):
                if sibling.strip():
                    sibling_texts.append(sibling.strip())
            else:
                t = sibling.get_text(strip=True)
                if t:
                    sibling_texts.append(t)

        if sibling_texts:
            text = ' '.join(sibling_texts)

        if len(text) < 20:
            text = parent.get_text(strip=True)

        if len(text) < 20:
            ancestor = parent
            for _ in range(3):
                if ancestor.parent:
                    ancestor = ancestor.parent
                    t = ancestor.get_text(strip=True)
                    if len(t) > 20:
                        text = t
                        break

        if not re.search(r'[\u4e00-\u9fffA-Za-z]', text):
            print(f"[跳过] 文本不包含有效字符：{text[:50]}")
            continue

        # 附加 source_url 字段，方便追踪来源
        data.append({'image_url': img_url, 'text': text, 'source_url': url})
        print(f"[调试] 提取图文对数量: {len(data)} from {url}")

    return data

def upload_to_cloudinary(img_url, text, max_retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://zh.wikipedia.org/"
    }
    for attempt in range(max_retries):
        try:
            response = requests.get(img_url, headers=headers, timeout=10)
            if response.status_code != 200:
                print(f"[图片请求失败] 状态码: {response.status_code} URL: {img_url}")
                return None

            content_type = response.headers.get("Content-Type", "")
            if not content_type.startswith("image/"):
                print(f"[跳过] 非图片资源: {img_url}")
                return None

            if len(response.content) < 1024:
                print(f"[跳过] 图片太小: {img_url}")
                return None

            if len(response.content) > 5 * 1024 * 1024:
                print(f"[跳过] 图片过大: {img_url}")
                return None

            filename = str(uuid.uuid4())
            result = cloudinary.uploader.upload(
                BytesIO(response.content),
                folder=CLOUD_FOLDER,
                public_id=filename,
                resource_type="image"
            )

            image_url = result.get('secure_url') or result.get('url')
            if image_url:
                return {'image_url': image_url, 'text': text.strip()}
            else:
                print(f"[上传失败] Cloudinary返回结果无URL: {img_url}")
                return None

        except Exception as e:
            print(f"[上传尝试{attempt+1}失败] {img_url} 错误: {e}")
            time.sleep(2)

    print(f"[上传失败] 多次尝试未成功: {img_url}")
    return None

def run_crawler(start_input, threads):
    print(f"启动任务：{start_input}")
    all_tasks = []

    if start_input.startswith("search:"):
        keywords = start_input.replace("search:", "").strip().split()
        for keyword in keywords:
            if SEARCH_ENGINE == 'baidu':
                url = f"https://www.baidu.com/s?wd={keyword}+图片"
            elif SEARCH_ENGINE == 'bing':
                url = f"https://www.bing.com/images/search?q={keyword}"
            elif SEARCH_ENGINE == 'google':
                url = f"https://www.google.com/search?tbm=isch&q={keyword}"
            else:
                print(f"[警告] 不支持的搜索引擎: {SEARCH_ENGINE}")
                continue
            all_tasks.append((url, keyword))
    else:
        all_tasks.append((start_input, '自定义网址'))

    all_pairs = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_url = {executor.submit(fetch_and_extract, url): (url, kw) for url, kw in all_tasks}
        for future in tqdm(as_completed(future_to_url), total=len(future_to_url), desc="爬取网页"):
            url, keyword = future_to_url[future]
            try:
                pairs = future.result()
                print(f"[{keyword}] 爬取到 {len(pairs)} 个图文对")
                all_pairs.extend(pairs)
            except Exception as e:
                print(f"[{keyword}] 爬取任务异常: {e}")

    # 轻量简单的按 image_url 去重，避免完全重复
    if FILTER_DUPLICATES:
        seen_urls = set()
        filtered = []
        for pair in all_pairs:
            if pair['image_url'] not in seen_urls:
                filtered.append(pair)
                seen_urls.add(pair['image_url'])
        all_pairs = filtered

    all_pairs = all_pairs[:MAX_IMAGES]

    output_data = []
    os.makedirs(os.path.dirname(OUTPUT_JSONL), exist_ok=True)

    for pair in tqdm(all_pairs, desc="上传图片"):
        print(f"\n[准备上传] 图片：{pair['image_url']}\n文本：{pair['text'][:50]}...")
        cloud_res = upload_to_cloudinary(pair['image_url'], pair['text'])
        if cloud_res:
            output_data.append({
                'image_url': cloud_res['image_url'],
                'text': cloud_res['text'],
                'source_url': pair.get('source_url', '')
            })

    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for item in output_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

    print(f"任务完成，共上传图文对：{len(output_data)}")
    return f"共处理网址 {len(all_tasks)} 个，成功上传 {len(output_data)} 个图文对。"
