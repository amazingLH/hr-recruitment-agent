"""
* 文件作用
  * 这是知识助理模块的通用文本处理工具集，把文档解析后经常重复使用的纯文本操作封装为无状态函数。它涵盖 DOCX 相邻行去重、空白与控制字符清洗、HTML 标签移除、文本规范化与截断、关键词提取、固定长度分块、选项解析、文本相似度、文件大小格式化、Markdown 转义、slug 生成、敏感信息脱敏，以及多种文件格式的正文提取。多数函数使用正则表达式、字符串方法、集合去重和第三方文档解析库，适合用来理解进入 Embedding 之前为什么必须先把噪声、重复内容和格式差异处理掉。
* 类说明
  * 本文件没有定义业务类，而是以模块级函数组织接口或工具能力；这样可以直接被 FastAPI 路由注册或被其他模块按函数导入调用。
* 函数与方法说明
  * `_deduplicate_adjacent_docx_lines()`：合并 DOCX 解析中因合并单元格或兼容 XML 产生的相邻重复行。内部调用 re.sub() 完成主要工作，使用 正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `clean_text()`：清理和规范化文本： 用于把原始文本变得更干净。内部调用 re.sub() 完成主要工作，使用 正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `remove_html_tags()`：从文本中移除HTML标签。内部调用 re.sub() 完成主要工作，使用 正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `normalize_text()`：规范化文本用于搜索和比较。内部调用 re.sub() 完成主要工作，使用 正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `truncate_text()`：将文本截断到最大长度并添加后缀： 限制文本长度 如果文本超过max_length，它会保留后缀空间 并且会尝试在单词边界处截断，避免把英文单词截成一半。
  * `extract_keywords()`：从文本中提取关键词（简单实现）。
  * `split_text_into_chunks()`：文本分块： 用于把长文本切成多个块，默认每块1000 字符，块之间重叠100 字符。 会尽量在句号、感叹号、问号处截断；如果找不到合适句子边界，就尝试在空格处截断。 用于知识库/RAG，把长文档切成适合向量化和检索的小片段。
  * `split_options()`：按 分号分隔符 分割选项 字符串（支持英文和中文分号） 例如： "A选项；B选项; C选项" 分隔后： ["A选项", "B选项", "C选项"] 处理考试题选项、问卷选项等场景。
  * `calculate_text_similarity()`：文本相似度： 基于共同词汇计算简单的文本相似度（Jaccard 相似度）。
  * `format_file_size()`：文件大小格式化：以人类可读的格式格式化文件大小。
  * `escape_markdown()`：Markdown 转义： 转义Markdown特殊字符 会在字符前加反斜杠，避免用户输入被 Markdown渲染成格式。
  * `generate_slug()`：生成 URL slug： 从文本生成URL友好的slug 例如："Senior Python Developer!" => "senior-python-developer"。内部调用 re.sub() 完成主要工作，使用 正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `mask_sensitive_data()`：敏感数据脱敏: 屏蔽敏感数据，电子邮件、电话号码 例如： john@example.com => jo************om。内部调用 re.sub() 完成主要工作，使用 正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `extract_text_content()`：从文件提取文本: 这是一个异步函数，用于从不同类型文件中提取文本。 这镜像了增强文档服务的提取逻辑，以便可以在知识库摄取和简历筛选之间重用。
"""

# 补充当前字符串、提示词或响应正文内容。
""""""
# 导入 `re`，供后续代码调用其功能。
import re
# 导入 `html`，供后续代码调用其功能。
import html
# 从 `typing` 导入当前模块需要的 `List, Optional`。
from typing import List, Optional
# 导入 `unicodedata`，供后续代码调用其功能。
import unicodedata
# 导入 `logging`，供后续代码调用其功能。
import logging

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)

"""
    text_utils.py 是文本处理工具模块
        主要用于文本清洗、分词关键词提取、文本分块、相似度计算、Markdown转义、敏感信息脱敏，以及从上传文件里提取文本。
        
    主要服务于知识库、简历解析、考试题处理、搜索匹配和展示层，把常见文本清洗、切块、提取、格式化逻辑集中封装。
"""


# 定义 `_deduplicate_adjacent_docx_lines()`，执行该函数对应的业务流程。
def _deduplicate_adjacent_docx_lines(text: str) -> str:
    # 补充当前字符串、提示词或响应正文内容。
    """合并 DOCX 解析中因合并单元格或兼容 XML 产生的相邻重复行。"""
    # 计算或设置 `lines`，用于保存后续流程需要的中间结果。
    lines = text.splitlines()
    # 声明参数或字段 `deduplicated`，表示当前业务需要的数据。
    deduplicated: List[str] = []
    # 计算或设置 `previous_nonempty`，用于保存后续流程需要的中间结果。
    previous_nonempty = ""

    # 遍历当前集合中的每一项，逐项完成相同处理。
    for line in lines:
        # 计算或设置 `normalized`，用于保存后续流程需要的中间结果。
        normalized = re.sub(r"\s+", " ", line).strip()
        # 判断 `normalized and normalized == previous_nonempty` 是否成立，以选择正确的处理分支。
        if normalized and normalized == previous_nonempty:
            # 跳过当前项并继续处理下一项。
            continue
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        deduplicated.append(line)
        # 判断 `normalized` 是否成立，以选择正确的处理分支。
        if normalized:
            # 计算或设置 `previous_nonempty`，用于保存后续流程需要的中间结果。
            previous_nonempty = normalized

    # 结束当前函数，并把处理结果返回给调用方。
    return "\n".join(deduplicated).strip()



# 定义 `clean_text()`，执行该函数对应的业务流程。
def clean_text(text: str) -> str:
    """
        清理和规范化文本： 用于把原始文本变得更干净
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return ""

    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = remove_html_tags(text)  # 移除HTML标签
    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = unicodedata.normalize('NFKC', text)  # 规范化unicode
    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = re.sub(r'\s+', ' ', text)  # 移除多余空白字符
    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = text.strip()  # 移除首尾空白字符

    # 结束当前函数，并把处理结果返回给调用方。
    return text


# 定义 `remove_html_tags()`，执行该函数对应的业务流程。
def remove_html_tags(text: str) -> str:
    """
        从文本中移除HTML标签
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return ""

    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = html.unescape(text)  # 首先反转义HTML实体
    # 计算或设置 `clean`，用于保存后续流程需要的中间结果。
    clean = re.compile('<.*?>')  # 移除HTML标签
    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = re.sub(clean, '', text)

    # 结束当前函数，并把处理结果返回给调用方。
    return text


# 定义 `normalize_text()`，执行该函数对应的业务流程。
def normalize_text(text: str) -> str:
    """
        规范化文本用于搜索和比较
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return ""

    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = text.lower()  # 转换为小写
    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = re.sub(r'[^\w\s]', ' ', text)  # 移除标点符号和特殊字符
    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = re.sub(r'\s+', ' ', text)  # 移除多余空白字符
    # 计算或设置 `text`，用于保存后续流程需要的中间结果。
    text = text.strip()  # 去掉首尾空白

    # 结束当前函数，并把处理结果返回给调用方。
    return text


# 定义 `truncate_text()`，执行该函数对应的业务流程。
def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """
        将文本截断到最大长度并添加后缀： 限制文本长度
            如果文本超过max_length，它会保留后缀空间
            并且会尝试在单词边界处截断，避免把英文单词截成一半
    """
    # 判断 `not text or len(text) <= max_length` 是否成立，以选择正确的处理分支。
    if not text or len(text) <= max_length:
        # 结束当前函数，并把处理结果返回给调用方。
        return text

    # 计算或设置 `truncate_length`，用于保存后续流程需要的中间结果。
    truncate_length = max_length - len(suffix)  # 考虑后缀长度
    # 判断 `truncate_length <= 0` 是否成立，以选择正确的处理分支。
    if truncate_length <= 0:
        # 结束当前函数，并把处理结果返回给调用方。
        return suffix[:max_length]

    # 尝试在单词边界处截断
    # 计算或设置 `truncated`，用于保存后续流程需要的中间结果。
    truncated = text[:truncate_length]
    # 计算或设置 `last_space`，用于保存后续流程需要的中间结果。
    last_space = truncated.rfind(' ')

    # 如果我们能保留80%的文本
    # 判断 `last_space > truncate_length * 0.8` 是否成立，以选择正确的处理分支。
    if last_space > truncate_length * 0.8:
        # 计算或设置 `truncated`，用于保存后续流程需要的中间结果。
        truncated = truncated[:last_space]

    # 结束当前函数，并把处理结果返回给调用方。
    return truncated + suffix


# 定义 `extract_keywords()`，执行该函数对应的业务流程。
def extract_keywords(text: str, max_keywords: int = 10) -> List[str]:
    """
        从文本中提取关键词（简单实现）
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return []

    # 计算或设置 `normalized`，用于保存后续流程需要的中间结果。
    normalized = normalize_text(text)  # 规范化文本
    # 计算或设置 `words`，用于保存后续流程需要的中间结果。
    words = normalized.split()  # 分割成单词

    # 过滤常见停用词（简单列表）
    # 开始组装多行调用参数、集合或对象内容。
    stop_words = {
        # 补充当前字符串、提示词或响应正文内容。
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        # 补充当前字符串、提示词或响应正文内容。
        'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'have',
        # 补充当前字符串、提示词或响应正文内容。
        'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
        # 补充当前字符串、提示词或响应正文内容。
        'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those',
        # 补充当前字符串、提示词或响应正文内容。
        'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him', 'her',
        # 补充当前字符串、提示词或响应正文内容。
        'us', 'them', 'my', 'your', 'his', 'our', 'their'
    # 结束上方多行参数、集合或对象的定义。
    }

    # 过滤单词：过滤长度小于等于2的词
    # 计算或设置 `keywords`，用于保存后续流程需要的中间结果。
    keywords = []
    # 遍历当前集合中的每一项，逐项完成相同处理。
    for word in words:
        # 判断 `(len(word) > 2 and` 是否成立，以选择正确的处理分支。
        if (len(word) > 2 and
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            word not in stop_words and
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            word.isalpha()):
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            keywords.append(word)

    # 统计词频：统计频率并获取最常见的词
    # 计算或设置 `word_freq`，用于保存后续流程需要的中间结果。
    word_freq = {}
    # 遍历当前集合中的每一项，逐项完成相同处理。
    for word in keywords:
        # 计算或设置 `word_freq[word]`，用于保存后续流程需要的中间结果。
        word_freq[word] = word_freq.get(word, 0) + 1

    # 返回出现频率最高的前max_keywords个词
    # 计算或设置 `sorted_words`，用于保存后续流程需要的中间结果。
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)

    # 结束当前函数，并把处理结果返回给调用方。
    return [word for word, freq in sorted_words[:max_keywords]]


# 定义 `split_text_into_chunks()`，执行该函数对应的业务流程。
def split_text_into_chunks(
    # 声明参数或字段 `text`，表示当前业务需要的数据。
    text: str,
    # 声明参数或字段 `chunk_size`，表示当前业务需要的数据。
    chunk_size: int = 1000,
    # 声明参数或字段 `overlap`，表示当前业务需要的数据。
    overlap: int = 100
# 结束上方多行参数、集合或对象的定义。
) -> List[str]:
    """
        文本分块：
            用于把长文本切成多个块，默认每块1000 字符，块之间重叠100 字符。
            会尽量在句号、感叹号、问号处截断；如果找不到合适句子边界，就尝试在空格处截断。
            用于知识库/RAG，把长文档切成适合向量化和检索的小片段。
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return []

    # 判断 `len(text) <= chunk_size` 是否成立，以选择正确的处理分支。
    if len(text) <= chunk_size:
        # 结束当前函数，并把处理结果返回给调用方。
        return [text]

    # 计算或设置 `chunks`，用于保存后续流程需要的中间结果。
    chunks = []
    # 计算或设置 `start`，用于保存后续流程需要的中间结果。
    start = 0

    # 在条件成立期间重复执行当前处理逻辑。
    while start < len(text):
        # 计算或设置 `end`，用于保存后续流程需要的中间结果。
        end = start + chunk_size

        # 如果不是最后一个块，尝试在句子边界处结束
        # 判断 `end < len(text)` 是否成立，以选择正确的处理分支。
        if end < len(text):
            # 查找句子结尾
            sentence_end = text.rfind('.', start, end)
            # 判断 `sentence_end == -1` 是否成立，以选择正确的处理分支。
            if sentence_end == -1:
                # 计算或设置 `sentence_end`，用于保存后续流程需要的中间结果。
                sentence_end = text.rfind('!', start, end)
            # 判断 `sentence_end == -1` 是否成立，以选择正确的处理分支。
            if sentence_end == -1:
                # 计算或设置 `sentence_end`，用于保存后续流程需要的中间结果。
                sentence_end = text.rfind('?', start, end)

            # 如果找到句子结尾，使用它
            # 判断 `sentence_end > start + chunk_size * 0.5` 是否成立，以选择正确的处理分支。
            if sentence_end > start + chunk_size * 0.5:
                # 计算或设置 `end`，用于保存后续流程需要的中间结果。
                end = sentence_end + 1
            # 前面的判断条件均不成立时，执行当前备用分支。
            else:
                # 否则，尝试在单词边界处结束
                space_pos = text.rfind(' ', start, end)
                # 判断 `space_pos > start + chunk_size * 0.5` 是否成立，以选择正确的处理分支。
                if space_pos > start + chunk_size * 0.5:
                    # 计算或设置 `end`，用于保存后续流程需要的中间结果。
                    end = space_pos

        # 计算或设置 `chunk`，用于保存后续流程需要的中间结果。
        chunk = text[start:end].strip()
        # 判断 `chunk` 是否成立，以选择正确的处理分支。
        if chunk:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            chunks.append(chunk)

        # 移动起始位置（带重叠）
        # 计算或设置 `start`，用于保存后续流程需要的中间结果。
        start = end - overlap

        # 确保不会倒退
        # 判断 `start <= chunks[-1] if chunks else 0` 是否成立，以选择正确的处理分支。
        if start <= chunks[-1] if chunks else 0:
            # 计算或设置 `start`，用于保存后续流程需要的中间结果。
            start = end

    # 结束当前函数，并把处理结果返回给调用方。
    return chunks


# 定义 `split_options()`，执行该函数对应的业务流程。
def split_options(options_str: str) -> List[str]:
    """
        按 分号分隔符 分割选项 字符串（支持英文和中文分号）
        例如： "A选项；B选项; C选项"   分隔后： ["A选项", "B选项", "C选项"]

        处理考试题选项、问卷选项等场景。
    """
    # 判断 `not options_str` 是否成立，以选择正确的处理分支。
    if not options_str:
        # 结束当前函数，并把处理结果返回给调用方。
        return []

    # 按分号分割（英文和中文）并清理每个选项
    options = [opt.strip() for opt in re.split(r'[;；]', options_str) if opt.strip()]
    # 结束当前函数，并把处理结果返回给调用方。
    return options


# 定义 `calculate_text_similarity()`，执行该函数对应的业务流程。
def calculate_text_similarity(text1: str, text2: str) -> float:
    """
        文本相似度： 基于共同词汇计算简单的文本相似度（Jaccard 相似度）
    """
    # 判断 `not text1 or not text2` 是否成立，以选择正确的处理分支。
    if not text1 or not text2:
        # 结束当前函数，并把处理结果返回给调用方。
        return 0.0

    # 规范化文本
    # 计算或设置 `words1`，用于保存后续流程需要的中间结果。
    words1 = set(normalize_text(text1).split())
    # 计算或设置 `words2`，用于保存后续流程需要的中间结果。
    words2 = set(normalize_text(text2).split())

    # 判断 `not words1 or not words2` 是否成立，以选择正确的处理分支。
    if not words1 or not words2:
        # 结束当前函数，并把处理结果返回给调用方。
        return 0.0

    # 计算Jaccard相似度
    # 计算或设置 `intersection`，用于保存后续流程需要的中间结果。
    intersection = len(words1.intersection(words2))
    # 计算或设置 `union`，用于保存后续流程需要的中间结果。
    union = len(words1.union(words2))

    # 结束当前函数，并把处理结果返回给调用方。
    return intersection / union if union > 0 else 0.0


# 定义 `format_file_size()`，执行该函数对应的业务流程。
def format_file_size(size_bytes: int) -> str:
    """
        文件大小格式化：以人类可读的格式格式化文件大小
    """
    # 判断 `size_bytes == 0` 是否成立，以选择正确的处理分支。
    if size_bytes == 0:
        # 结束当前函数，并把处理结果返回给调用方。
        return "0 B"

    # 计算或设置 `size_names`，用于保存后续流程需要的中间结果。
    size_names = ["B", "KB", "MB", "GB", "TB"]
    # 计算或设置 `i`，用于保存后续流程需要的中间结果。
    i = 0
    # 计算或设置 `size`，用于保存每页记录数量。
    size = float(size_bytes)

    # 在条件成立期间重复执行当前处理逻辑。
    while size >= 1024.0 and i < len(size_names) - 1:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        size /= 1024.0
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        i += 1

    # 结束当前函数，并把处理结果返回给调用方。
    return f"{size:.1f} {size_names[i]}"


# 定义 `escape_markdown()`，执行该函数对应的业务流程。
def escape_markdown(text: str) -> str:
    """
        Markdown 转义： 转义Markdown特殊字符
            会在字符前加反斜杠，避免用户输入被 Markdown渲染成格式。
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return ""

    # Markdown中需要转义的字符
    escape_chars = ['\\', '`', '*', '_', '{', '}', '[', ']', '(', ')', '#', '+', '-', '.', '!']

    # 遍历当前集合中的每一项，逐项完成相同处理。
    for char in escape_chars:
        # 计算或设置 `text`，用于保存后续流程需要的中间结果。
        text = text.replace(char, f'\\{char}')

    # 结束当前函数，并把处理结果返回给调用方。
    return text


# 定义 `generate_slug()`，执行该函数对应的业务流程。
def generate_slug(text: str, max_length: int = 50) -> str:
    """
        生成 URL slug： 从文本生成URL友好的slug
            例如："Senior Python Developer!"  => "senior-python-developer"
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return ""

    # 转换为小写
    # 计算或设置 `slug`，用于保存后续流程需要的中间结果。
    slug = text.lower()
    # 计算或设置 `slug`，用于保存后续流程需要的中间结果。
    slug = unicodedata.normalize('NFKD', slug)


    # 计算或设置 `slug`，用于保存后续流程需要的中间结果。
    slug = slug.encode('ascii', 'ignore').decode('ascii')  # 移除非ASCII字符
    # 计算或设置 `slug`，用于保存后续流程需要的中间结果。
    slug = re.sub(r'[^a-z0-9]+', '-', slug)  # 用连字符替换空格和特殊字符
    # 计算或设置 `slug`，用于保存后续流程需要的中间结果。
    slug = slug.strip('-')  # 移除首尾连字符

    # 如果太长则截断
    # 判断 `len(slug) > max_length` 是否成立，以选择正确的处理分支。
    if len(slug) > max_length:
        # 计算或设置 `slug`，用于保存后续流程需要的中间结果。
        slug = slug[:max_length].rstrip('-')

    # 结束当前函数，并把处理结果返回给调用方。
    return slug or "untitled"


# 定义 `mask_sensitive_data()`，执行该函数对应的业务流程。
def mask_sensitive_data(text: str, mask_char: str = "*") -> str:
    """
        敏感数据脱敏: 屏蔽敏感数据，电子邮件、电话号码
            例如： john@example.com  =>  jo************om
    """
    # 判断 `not text` 是否成立，以选择正确的处理分支。
    if not text:
        # 结束当前函数，并把处理结果返回给调用方。
        return ""

    # 屏蔽电子邮件地址
    # 开始组装多行调用参数、集合或对象内容。
    text = re.sub(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        lambda m: m.group(0)[:2] + mask_char * (len(m.group(0)) - 4) + m.group(0)[-2:],
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        text
    # 结束上方多行参数、集合或对象的定义。
    )

    # 屏蔽电话号码（简单模式）
    # 开始组装多行调用参数、集合或对象内容。
    text = re.sub(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        lambda m: mask_char * len(m.group(0)),
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        text
    # 结束上方多行参数、集合或对象的定义。
    )

    # 结束当前函数，并把处理结果返回给调用方。
    return text


# 定义 `extract_text_content()`，执行该函数对应的业务流程。
async def extract_text_content(file_path: str, mime_type: str) -> str:
    """
        从文件提取文本:
            这是一个异步函数，用于从不同类型文件中提取文本。
            这镜像了增强文档服务的提取逻辑，以便可以在知识库摄取和简历筛选之间重用。
    """
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 对于txt/md：直接用 UTF-8读取文件
        if mime_type in ('text/plain', 'text/markdown'):
            # 通过上下文管理器安全取得并在结束时释放当前资源。
            with open(file_path, 'r', encoding='utf-8') as f:
                # 结束当前函数，并把处理结果返回给调用方。
                return f.read()

        # 对于PDF：尝试导入PyPDF2，逐页调用，拼接成完整文本。
        elif mime_type == 'application/pdf':
            # 计算或设置 `text`，用于保存后续流程需要的中间结果。
            text = ""
            # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
            try:
                # 导入 `PyPDF2  # type: ignore`，供后续代码调用其功能。
                import PyPDF2  # type: ignore
            # 捕获指定异常，避免底层错误未经处理直接中断请求。
            except Exception as e:
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                logger.error(f"PyPDF2不可用于PDF提取: {e}")
                # 结束当前函数，并把处理结果返回给调用方。
                return ""

            # 通过上下文管理器安全取得并在结束时释放当前资源。
            with open(file_path, 'rb') as f:
                # 计算或设置 `pdf_reader`，用于保存后续流程需要的中间结果。
                pdf_reader = PyPDF2.PdfReader(f)
                # 遍历当前集合中的每一项，逐项完成相同处理。
                for page in pdf_reader.pages:
                    # 计算或设置 `page_text`，用于保存后续流程需要的中间结果。
                    page_text = page.extract_text() or ""
                    # 判断 `page_text` 是否成立，以选择正确的处理分支。
                    if page_text:
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        text += page_text + "\n"
            # 结束当前函数，并把处理结果返回给调用方。
            return text.strip()


        # 判断 `mime_type in ('application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document')` 是否成立，以选择正确的处理分支。
        elif mime_type in ('application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'):
            # 判断 `mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'` 是否成立，以选择正确的处理分支。
            if mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':

                # 对于DOCX：优先使用docx2txt
                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 导入 `docx2txt  # type: ignore`，供后续代码调用其功能。
                    import docx2txt  # type: ignore
                    # 计算或设置 `extracted`，用于保存后续流程需要的中间结果。
                    extracted = docx2txt.process(file_path) or ""
                    # 判断 `extracted.strip()` 是否成立，以选择正确的处理分支。
                    if extracted.strip():
                        # 记录当前执行状态或异常信息，便于开发和运行排查。
                        logger.info("使用docx2txt提取DOCX内容")
                        # 结束当前函数，并把处理结果返回给调用方。
                        return _deduplicate_adjacent_docx_lines(extracted)
                    # 前面的判断条件均不成立时，执行当前备用分支。
                    else:
                        # 记录当前执行状态或异常信息，便于开发和运行排查。
                        logger.info("docx2txt返回空；回退到python-docx")
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except Exception as e:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.info(f"docx2txt不可用或失败({e})；回退到python-docx")

                # 如果docx2txt不可用或提取为空，用python-docx，读取段落和表格单元格文本
                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 从 `docx` 导入当前模块需要的 `Document as DocxDocument`。
                    from docx import Document as DocxDocument
                    # 计算或设置 `doc`，用于保存后续流程需要的中间结果。
                    doc = DocxDocument(file_path)
                    # 计算或设置 `parts`，用于保存后续流程需要的中间结果。
                    parts = []
                    # 遍历当前集合中的每一项，逐项完成相同处理。
                    for p in doc.paragraphs:
                        # 判断 `p.text and p.text.strip()` 是否成立，以选择正确的处理分支。
                        if p.text and p.text.strip():
                            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                            parts.append(p.text)
                    # 遍历当前集合中的每一项，逐项完成相同处理。
                    for table in doc.tables:
                        # 计算或设置 `seen_cells`，用于保存后续流程需要的中间结果。
                        seen_cells = set()
                        # 遍历当前集合中的每一项，逐项完成相同处理。
                        for row in table.rows:
                            # 遍历当前集合中的每一项，逐项完成相同处理。
                            for cell in row.cells:
                                # 计算或设置 `cell_key`，用于保存后续流程需要的中间结果。
                                cell_key = id(cell._tc)
                                # 判断 `cell_key in seen_cells` 是否成立，以选择正确的处理分支。
                                if cell_key in seen_cells:
                                    # 跳过当前项并继续处理下一项。
                                    continue
                                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                                seen_cells.add(cell_key)
                                # 判断 `cell.text and cell.text.strip()` 是否成立，以选择正确的处理分支。
                                if cell.text and cell.text.strip():
                                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                                    parts.append(cell.text)
                    # 结束当前函数，并把处理结果返回给调用方。
                    return _deduplicate_adjacent_docx_lines("\n".join(parts))
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except Exception as e:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.error(f"python-docx提取DOCX内容失败: {e}")
                    # 结束当前函数，并把处理结果返回给调用方。
                    return ""

            # .doc旧格式：此处不支持，需要外部工具
            logger.warning(f"Unsupported .doc format for direct extraction: {mime_type}")
            # 结束当前函数，并把处理结果返回给调用方。
            return ""

        # 前面的判断条件均不成立时，执行当前备用分支。
        else:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.warning(f"不支持的文件类型: {mime_type}")
            # 结束当前函数，并把处理结果返回给调用方。
            return ""

    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 记录当前执行状态或异常信息，便于开发和运行排查。
        logger.error(f"从{file_path}提取文本时出错: {e}")
        # 结束当前函数，并把处理结果返回给调用方。
        return ""
