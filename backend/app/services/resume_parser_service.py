"""
简历文件校验与文本提取服务。

【文件定位】
本文件是简历筛选的文件处理层，不负责 AI 评分。它先校验文件大小和扩展名，再借助
公共文本工具从 PDF、TXT、DOC、DOCX 中提取纯文本，并提供文件哈希、MIME 类型和
基础信息，供 ResumeEvaluationService 保存和送入大模型。

【主要函数】
- __init__：设置简历文件最大值为 10MB，作为所有上传入口的统一限制。
- validate_file：检查文件大小以及 .pdf、.txt、.doc、.docx 扩展名，返回“是否通过、原因”。
- extract_text_from_file：识别 MIME 类型，把上传字节临时落盘，调用公共 extract_text_content
  提取正文，并在 finally 中删除临时文件。
- _extract_from_txt：兼容多种编码读取 TXT 简历，避免中文文本因编码不同而解析失败。
- _extract_from_pdf：使用 PDF 解析能力逐页提取文字并合并为简历正文。
- _extract_from_doc：把旧版 DOC 临时落盘，依次尝试 textract、Windows Word COM 和 antiword；
  多种方案都失败时给出缺少可用解析器的明确错误，并清理临时文件。
- _extract_from_docx：读取 DOCX 中的段落和表格文字，组成可供 AI 使用的完整正文。
- get_file_info：根据文件名和字节生成原文件名、大小、扩展名、MIME 类型及 SHA-256 哈希等元数据。

【阅读重点】
bytes 表示上传文件的二进制内容；Tuple[bool, str] 表示同时返回是否成功和说明文字；
finally 无论成功失败都会执行，适合保证临时文件被删除。
"""

# 引入依赖，用于记录业务执行过程和失败原因。
import logging
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import os
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import hashlib
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import tempfile
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from typing import BinaryIO, Tuple, Optional
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from uuid import UUID
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import mimetypes
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.utils.text_utils import extract_text_content
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.utils.file_utils import get_file_mime_type

# 创建模块日志记录器，统一记录业务过程和异常。
logger = logging.getLogger(__name__)


# 集中管理 `ResumeParserService` 对应模块的状态和业务方法，避免逻辑散落在接口层。
class ResumeParserService:
    """用于解析简历文件和提取文本内容的服务"""
    
    # 把 `SUPPORTED_EXTENSIONS` 字段写入当前请求、响应或数据对象，供 `当前流程` 后续步骤使用。
    SUPPORTED_EXTENSIONS = {'.pdf', '.txt', '.doc', '.docx'}
    # 把 `SUPPORTED_MIME_TYPES` 字段写入当前请求、响应或数据对象，供 `当前流程` 后续步骤使用。
    SUPPORTED_MIME_TYPES = {
        # 向 `当前流程` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
        'application/pdf',
        # 向 `当前流程` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
        'text/plain',
        # 向 `当前流程` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
        'application/msword',
        # 向 `当前流程` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    # 完成“完成当前模块初始化”所需的数据封装或下游调用，准备处理其结果。
    }

    # 封装“完成 `init` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def __init__(self):
        # 保存当前对象的 `max_file_size` 业务状态，供同一服务的其他方法复用。
        self.max_file_size = 10 * 1024 * 1024  # 10MB
    
    # 封装“校验简历文件是否允许上传”流程，供接口层或其他业务服务调用。
    def validate_file(self, filename: str, file_size: int) -> Tuple[bool, str]:
        """验证文件是否符合要求"""
        # 检查文件大小
        if file_size > self.max_file_size:
            # 把“校验简历文件是否允许上传”得到的最终业务结果返回给接口层或前端。
            return False, f"文件大小超过限制 ({self.max_file_size / 1024 / 1024}MB)"
        
        # 检查文件扩展名
        _, ext = os.path.splitext(filename.lower())
        # 根据 `ext not in self.SUPPORTED_EXTENSIONS` 判断当前数据应进入哪条业务分支，保证 `validate_file` 使用正确处理方式。
        if ext not in self.SUPPORTED_EXTENSIONS:
            # 把“校验简历文件是否允许上传”得到的最终业务结果返回给接口层或前端。
            return False, f"不支持的文件格式。支持的格式: {', '.join(self.SUPPORTED_EXTENSIONS)}"
        
        # 把“校验简历文件是否允许上传”得到的最终业务结果返回给接口层或前端。
        return True, "文件验证通过"

    # 封装“从简历文件提取可供 AI 使用的正文”流程，供接口层或其他业务服务调用。
    async def extract_text_from_file(self, file_content: bytes, filename: str) -> str:
        """从文件中提取文本内容（使用公共提取方法）"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 统一通过公共方法提取，保持与知识库上传一致
            mime_type = get_file_mime_type(filename)

            # 写入临时文件以复用基于路径的解析逻辑
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as temp_file:
                # 调用 `temp_file.write` 执行 `extract_text_from_file` 的当前步骤，并将产生的数据交给后续流程。
                temp_file.write(file_content)
                # 记录临时简历文件位置，供公共文本解析器读取并在最后删除。
                temp_path = temp_file.name

            # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            try:
                # 调用 `extract_text_content` 执行“从简历文件提取可供 AI 使用的正文”所需的下游操作，并保存返回结果供下一步使用。
                extracted = await extract_text_content(temp_path, mime_type)
                # 把“从简历文件提取可供 AI 使用的正文”得到的最终业务结果返回给接口层或前端。
                return extracted
            # 无论解析成功还是失败都释放临时文件或网络资源，避免资源泄漏。
            finally:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 调用 `os.unlink` 执行 `extract_text_from_file` 的当前步骤，并将产生的数据交给后续流程。
                    os.unlink(temp_path)
                # 捕获“从简历文件提取可供 AI 使用的正文”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception:
                    # 引入或传递 `pass`，供“从简历文件提取可供 AI 使用的正文”的后续步骤使用。
                    pass
        # 捕获“从简历文件提取可供 AI 使用的正文”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“从简历文件提取可供 AI 使用的正文”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"提取文本内容失败: {e}")
            # 发现无法继续的业务问题，终止“从简历文件提取可供 AI 使用的正文”并交给上层统一处理。
            raise Exception(f"文件解析失败: {str(e)}")

    # 封装“完成 `extract from txt` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def _extract_from_txt(self, file_content: bytes) -> str:
        """从TXT文件提取文本"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 尝试不同的编码
            encodings = ['utf-8', 'utf-16', 'gbk', 'gb2312', 'big5', 'latin-1']
            
            # 逐项处理“完成 `extract from txt` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            for encoding in encodings:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 把“完成 `extract from txt` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    return file_content.decode(encoding)
                # 捕获“完成 `extract from txt` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except UnicodeDecodeError:
                    # 引入或传递 `continue`，供“完成 `extract from txt` 对应的业务处理”的后续步骤使用。
                    continue
            
            # 如果所有编码都失败，使用utf-8并忽略错误
            return file_content.decode('utf-8', errors='ignore')
            
        # 捕获“完成 `extract from txt` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“完成 `extract from txt` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"TXT文件解析失败: {e}")
            # 引入或传递 `raise`，供“完成 `extract from txt` 对应的业务处理”的后续步骤使用。
            raise

    # 封装“完成 `extract from pdf` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def _extract_from_pdf(self, file_content: bytes) -> str:
        """从PDF文件提取文本"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 使用PyPDF2或pdfplumber库
            import PyPDF2
            # 引入当前模块完成“完成 `extract from pdf` 对应的业务处理”需要的类型或工具。
            import io
            
            # 把 `pdf_reader` 字段写入当前请求、响应或数据对象，供 `_extract_from_pdf` 后续步骤使用。
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            # 把 `text_content` 字段写入当前请求、响应或数据对象，供 `_extract_from_pdf` 后续步骤使用。
            text_content = []
            
            # 逐项处理“完成 `extract from pdf` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            for page in pdf_reader.pages:
                # 把 `page_text` 字段写入当前请求、响应或数据对象，供 `_extract_from_pdf` 后续步骤使用。
                page_text = page.extract_text()
                # 根据 `page_text` 判断当前数据应进入哪条业务分支，保证 `_extract_from_pdf` 使用正确处理方式。
                if page_text:
                    # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                    text_content.append(page_text)
            
            # 把“完成 `extract from pdf` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return '\n'.join(text_content)
            
        # 捕获“完成 `extract from pdf` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ImportError:
            # 如果没有安装PyPDF2，尝试使用pdfplumber
            try:
                # 引入当前模块完成“完成 `extract from pdf` 对应的业务处理”需要的类型或工具。
                import pdfplumber
                # 引入当前模块完成“完成 `extract from pdf` 对应的业务处理”需要的类型或工具。
                import io
                
                # 安全打开临时文件或资源，并在处理结束后自动关闭。
                with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                    # 把 `text_content` 字段写入当前请求、响应或数据对象，供 `_extract_from_pdf` 后续步骤使用。
                    text_content = []
                    # 逐项处理“完成 `extract from pdf` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                    for page in pdf.pages:
                        # 把 `text` 字段写入当前请求、响应或数据对象，供 `_extract_from_pdf` 后续步骤使用。
                        text = page.extract_text()
                        # 根据 `text` 判断当前数据应进入哪条业务分支，保证 `_extract_from_pdf` 使用正确处理方式。
                        if text:
                            # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                            text_content.append(text)
                    
                    # 把“完成 `extract from pdf` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    return '\n'.join(text_content)
                    
            # 捕获“完成 `extract from pdf` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
            except ImportError:
                # 记录“完成 `extract from pdf` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                logger.error("PDF解析库未安装，请安装 PyPDF2 或 pdfplumber")
                # 发现无法继续的业务问题，终止“完成 `extract from pdf` 对应的业务处理”并交给上层统一处理。
                raise Exception("PDF解析功能不可用，请联系管理员")
                
        # 捕获“完成 `extract from pdf` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“完成 `extract from pdf` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"PDF文件解析失败: {e}")
            # 发现无法继续的业务问题，终止“完成 `extract from pdf` 对应的业务处理”并交给上层统一处理。
            raise Exception(f"PDF文件解析失败: {str(e)}")

    # 封装“完成 `extract from doc` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def _extract_from_doc(self, file_content: bytes) -> str:
        """从DOC文件提取文本"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 写入临时文件
            with tempfile.NamedTemporaryFile(suffix='.doc', delete=False) as temp_file:
                # 调用 `temp_file.write` 执行 `_extract_from_doc` 的当前步骤，并将产生的数据交给后续流程。
                temp_file.write(file_content)
                # 把 `temp_file_path` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                temp_file_path = temp_file.name

            # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            try:
                # 优先使用 textract（跨平台，支持DOC）
                try:
                    # 引入当前模块完成“完成 `extract from doc` 对应的业务处理”需要的类型或工具。
                    import textract
                    # 把 `text_bytes` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                    text_bytes = textract.process(temp_file_path)
                    # 尝试多编码解码，避免中文乱码
                    decoded = None
                    # 逐项处理“完成 `extract from doc` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                    for enc in ['utf-8', 'utf-16', 'gbk', 'gb2312', 'big5', 'latin-1']:
                        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                        try:
                            # 把 `decoded` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                            decoded = text_bytes.decode(enc)
                            # 引入或传递 `break`，供“完成 `extract from doc` 对应的业务处理”的后续步骤使用。
                            break
                        # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                        except Exception:
                            # 引入或传递 `continue`，供“完成 `extract from doc` 对应的业务处理”的后续步骤使用。
                            continue
                    # 根据 `decoded is None` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                    if decoded is None:
                        # 把 `decoded` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                        decoded = text_bytes.decode('utf-8', errors='ignore')
                    # 把 `text` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                    text = decoded.strip()
                    # 根据 `text` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                    if text:
                        # 把“完成 `extract from doc` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                        return text
                # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except ImportError:
                    # 记录“完成 `extract from doc` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning("未安装textract，尝试其他方式解析DOC")
                # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“完成 `extract from doc` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"textract解析DOC失败: {e}")

                # Windows 环境下尝试使用 Word COM 自动化
                try:
                    # 引入当前模块完成“完成 `extract from doc` 对应的业务处理”需要的类型或工具。
                    import win32com.client  # type: ignore
                    # 把 `word` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                    word = win32com.client.Dispatch("Word.Application")
                    # 为 `_extract_from_doc` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
                    word.Visible = False
                    # 把 `doc` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                    doc = word.Documents.Open(temp_file_path)
                    # 保存为临时txt
                    temp_txt_path = temp_file_path + ".txt"
                    # 把 `wdFormatText` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                    wdFormatText = 2
                    # 为 `_extract_from_doc` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
                    doc.SaveAs(temp_txt_path, FileFormat=wdFormatText)
                    # 调用 `doc.Close` 执行 `_extract_from_doc` 的当前步骤，并将产生的数据交给后续流程。
                    doc.Close(False)
                    # 调用 `word.Quit` 执行 `_extract_from_doc` 的当前步骤，并将产生的数据交给后续流程。
                    word.Quit()
                    # 读取时尝试多编码，避免中文乱码
                    text = None
                    # 逐项处理“完成 `extract from doc` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                    for enc in ['utf-8', 'utf-16', 'gbk', 'gb2312', 'big5', 'latin-1']:
                        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                        try:
                            # 安全打开临时文件或资源，并在处理结束后自动关闭。
                            with open(temp_txt_path, "r", encoding=enc) as f:
                                # 保存当前生成或解析出的业务正文，供后续展示或持久化。
                                content = f.read().strip()
                                # 根据 `content` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                                if content:
                                    # 把 `text` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                                    text = content
                                    # 引入或传递 `break`，供“完成 `extract from doc` 对应的业务处理”的后续步骤使用。
                                    break
                        # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                        except UnicodeDecodeError:
                            # 引入或传递 `continue`，供“完成 `extract from doc` 对应的业务处理”的后续步骤使用。
                            continue
                        # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                        except Exception:
                            # 引入或传递 `continue`，供“完成 `extract from doc` 对应的业务处理”的后续步骤使用。
                            continue
                    # 根据 `text is None` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                    if text is None:
                        # 安全打开临时文件或资源，并在处理结束后自动关闭。
                        with open(temp_txt_path, "r", encoding="utf-8", errors="ignore") as f:
                            # 把 `text` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                            text = f.read().strip()
                    # 调用 `os.unlink` 执行 `_extract_from_doc` 的当前步骤，并将产生的数据交给后续流程。
                    os.unlink(temp_txt_path)
                    # 根据 `text` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                    if text:
                        # 把“完成 `extract from doc` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                        return text
                # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except ImportError:
                    # 记录“完成 `extract from doc` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning("未安装pywin32，跳过Word COM方式")
                # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“完成 `extract from doc` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"Word COM方式解析DOC失败: {e}")

                # 如果系统安装了 antiword，尝试调用（强制UTF-8输出）
                try:
                    # 引入当前模块完成“完成 `extract from doc` 对应的业务处理”需要的类型或工具。
                    import subprocess
                    # 保存当前服务调用结果，供接口返回或继续处理。
                    result = subprocess.run(
                        # 把当前字段纳入 `_extract_from_doc` 的请求或响应结构，确保后续步骤获得完整业务数据。
                        ["antiword", "-m", "UTF-8", temp_file_path],
                        # 把 `stdout` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                        stdout=subprocess.PIPE,
                        # 把 `stderr` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                        stderr=subprocess.PIPE,
                        # 把 `check` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                        check=False
                    # 完成“完成 `extract from doc` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                    )
                    # 先按多编码尝试解码stdout
                    text = None
                    # 逐项处理“完成 `extract from doc` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                    for enc in ['utf-8', 'utf-16', 'gbk', 'gb2312', 'big5', 'latin-1']:
                        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                        try:
                            # 把 `text` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                            text = result.stdout.decode(enc).strip()
                            # 根据 `text` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                            if text:
                                # 引入或传递 `break`，供“完成 `extract from doc` 对应的业务处理”的后续步骤使用。
                                break
                        # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                        except Exception:
                            # 引入或传递 `continue`，供“完成 `extract from doc` 对应的业务处理”的后续步骤使用。
                            continue
                    # 根据 `text is None` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                    if text is None:
                        # 把 `text` 字段写入当前请求、响应或数据对象，供 `_extract_from_doc` 后续步骤使用。
                        text = result.stdout.decode('utf-8', errors='ignore').strip()
                    # 根据 `result.returncode == 0 and text` 判断当前数据应进入哪条业务分支，保证 `_extract_from_doc` 使用正确处理方式。
                    if result.returncode == 0 and text:
                        # 把“完成 `extract from doc` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                        return text
                    # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `_extract_from_doc` 能继续返回可处理结果。
                    else:
                        # 记录“完成 `extract from doc` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                        logger.warning(f"antiword解析失败: {result.stderr.decode('utf-8', errors='ignore')}")
                # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“完成 `extract from doc` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"调用antiword解析DOC失败: {e}")

                # 所有方式失败
                raise Exception("DOC文件解析失败：缺少可用的解析器，请安装textract或pywin32/antiword")

            # 无论解析成功还是失败都释放临时文件或网络资源，避免资源泄漏。
            finally:
                # 调用 `os.unlink` 执行 `_extract_from_doc` 的当前步骤，并将产生的数据交给后续流程。
                os.unlink(temp_file_path)

        # 捕获“完成 `extract from doc` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“完成 `extract from doc` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"DOC文件解析失败: {e}")
            # 发现无法继续的业务问题，终止“完成 `extract from doc` 对应的业务处理”并交给上层统一处理。
            raise Exception(f"DOC文件解析失败: {str(e)}")

    # 封装“完成 `extract from docx` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def _extract_from_docx(self, file_content: bytes) -> str:
        """从DOCX文件提取文本"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 引入当前模块完成“完成 `extract from docx` 对应的业务处理”需要的类型或工具。
            from docx import Document
            # 引入当前模块完成“完成 `extract from docx` 对应的业务处理”需要的类型或工具。
            import io
            
            # 从字节流创建Document对象
            doc = Document(io.BytesIO(file_content))
            
            # 提取所有段落的文本
            text_content = []
            # 逐项处理“完成 `extract from docx` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            for paragraph in doc.paragraphs:
                # 根据 `paragraph.text.strip()` 判断当前数据应进入哪条业务分支，保证 `_extract_from_docx` 使用正确处理方式。
                if paragraph.text.strip():
                    # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                    text_content.append(paragraph.text)
            
            # 提取表格中的文本
            for table in doc.tables:
                # 逐项处理“完成 `extract from docx` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                for row in table.rows:
                    # 逐项处理“完成 `extract from docx` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                    for cell in row.cells:
                        # 根据 `cell.text.strip()` 判断当前数据应进入哪条业务分支，保证 `_extract_from_docx` 使用正确处理方式。
                        if cell.text.strip():
                            # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                            text_content.append(cell.text)
            
            # 把“完成 `extract from docx` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return '\n'.join(text_content)
            
        # 捕获“完成 `extract from docx` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ImportError:
            # 记录“完成 `extract from docx` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error("DOCX解析库未安装，请安装 python-docx")
            # 发现无法继续的业务问题，终止“完成 `extract from docx` 对应的业务处理”并交给上层统一处理。
            raise Exception("DOCX文件解析功能不可用，请联系管理员")
            
        # 捕获“完成 `extract from docx` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“完成 `extract from docx` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"DOCX文件解析失败: {e}")
            # 发现无法继续的业务问题，终止“完成 `extract from docx` 对应的业务处理”并交给上层统一处理。
            raise Exception(f"DOCX文件解析失败: {str(e)}")

    # 封装“生成简历文件的元数据”流程，供接口层或其他业务服务调用。
    def get_file_info(self, filename: str, file_content: bytes) -> dict:
        """获取文件基本信息"""
        # 把 `file_size` 字段写入当前请求、响应或数据对象，供 `get_file_info` 后续步骤使用。
        file_size = len(file_content)
        # 保存文件 SHA-256 摘要，供文件识别和去重。
        file_hash = hashlib.sha256(file_content).hexdigest()
        # 为 `get_file_info` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        _, ext = os.path.splitext(filename.lower())
        
        # 把“生成简历文件的元数据”得到的最终业务结果返回给接口层或前端。
        return {
            # 向下游请求或返回结果写入 `filename` 业务字段。
            'filename': filename,
            # 向下游请求或返回结果写入 `file_type` 业务字段。
            'file_type': ext.lstrip('.'),
            # 向下游请求或返回结果写入 `file_size` 业务字段。
            'file_size': file_size,
            # 向下游请求或返回结果写入 `file_hash` 业务字段。
            'file_hash': file_hash,
            # 向下游请求或返回结果写入 `mime_type` 业务字段。
            'mime_type': mimetypes.guess_type(filename)[0]
        # 完成“生成简历文件的元数据”所需的数据封装或下游调用，准备处理其结果。
        }
