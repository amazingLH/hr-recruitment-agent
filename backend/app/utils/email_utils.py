"""
* 文件作用
  * 这是底层邮件协议工具。EmailReader 根据 EmailConfig 建立 IMAP 或 POP3 连接，完成登录、选择文件夹、搜索、读取和 MIME 邮件解析，并将正文、HTML 与附件统一转换成 EmailMessage；上层抓取服务不用直接处理协议细节。
* 类说明
  * `EmailConfig`：邮件协议层使用的轻量连接配置对象，只在内存中保存服务器、端口、账号、授权码、SSL 和 IMAP/POP3 协议，不映射数据库表。
  * `EmailMessage`：统一邮件消息容器，承载主题、发件人、收件人、时间、纯文本、HTML 和附件。
  * `EmailReader`：封装 IMAP/POP3 协议连接、搜索、读取、MIME 解析和连接释放的底层读取器。
* 主要函数与方法说明
  * `EmailConfig.__init__()`：保存邮件服务器地址、端口、账号、授权码、SSL 和协议，供 EmailReader 建立连接。
  * `EmailMessage.__init__()`：把解析后的邮件头、正文和附件保存为统一对象，附件缺失时初始化为空列表。
  * `EmailReader.__init__()`：保存连接配置并把 connection 初始化为空，连接成功后再写入 IMAP/POP3 客户端。
  * `EmailReader.connect()`：根据协议和 SSL 选择 IMAP4_SSL、IMAP4、POP3_SSL 或 POP3，使用账号与授权码登录；失败记录异常并返回 False。
  * `EmailReader.disconnect()`：按协议安全退出连接；IMAP 未选择邮箱时允许 close 失败但仍执行 logout。
  * `EmailReader.list_folders()`：向 IMAP 服务发送客户端 ID 后调用 list，解析并返回可用文件夹名称。
  * `EmailReader.select_folder()`：向 IMAP 服务发送客户端 ID 并选择指定文件夹，成功返回 True。
  * `EmailReader.search_emails()`：把搜索条件交给 IMAP search，解析空格分隔的邮件编号并转换成整数列表。
  * `EmailReader.get_email()`：按当前协议分发到 IMAP 或 POP3 单封邮件读取方法。
  * `EmailReader._get_imap_email()`：用 IMAP fetch 取得 RFC822 原始字节，解析为标准库 Message 后统一转换。
  * `EmailReader._get_pop3_email()`：用 POP3 retr 取得邮件各行，合并成原始字节后统一解析。
  * `EmailReader._parse_email_message()`：解析 MIME 邮件头和 multipart 内容，区分纯文本、HTML 与附件并返回 EmailMessage。
  * `EmailReader._decode_header_value()`：解析可能采用 Base64/Quoted-Printable 和不同字符集编码的邮件头，拼接成正常字符串。
  * `EmailReader.__enter__()`：支持 with 语法：进入上下文时连接邮箱，失败则抛出 ConnectionError。
  * `EmailReader.__exit__()`：退出 with 代码块时自动断开邮箱连接，避免连接泄漏。
"""

# 导入 imaplib，用于提供当前实现所需的标准库能力。
import imaplib
# 导入 platform，用于提供当前实现所需的标准库能力。
import platform
# 导入 poplib，用于提供当前实现所需的标准库能力。
import poplib
# 导入 smtplib，用于提供当前实现所需的标准库能力。
import smtplib
# 导入 email，用于提供当前实现所需的标准库能力。
import email
# 导入 socket，用于提供当前实现所需的标准库能力。
import socket
# 导入 decode_header，用于使用 decode_header 提供的类型或函数。
from email.header import decode_header
# 导入 Message，用于使用 Message 提供的类型或函数。
from email.message import Message
# 导入 List, Optional, Tuple，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import List, Optional, Tuple
# 导入 datetime，用于使用 datetime 提供的类型或函数。
from datetime import datetime
# 导入 logging，用于记录 Agent 规划、工具执行和降级异常。
import logging

# 调用 `logging.getLogger(__name__)` 完成当前解析、查询或对象构造，并把返回值保存到 `logger`；它表示当前模块的日志记录器，用于记录执行状态和异常。
logger = logging.getLogger(__name__)


# 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
"""
    email_utils.py 是一个邮件读取工具模块，主要用于连接IMAP/POP3邮箱服务器、搜索邮件、读取邮件内容、解析正文和附件。
    主要有三个类：
        EmailConfig：保存邮箱连接配置。
        EmailMessage：保存解析后的邮件内容。
        EmailReader：真正负责连接邮箱、搜索邮件、读取邮件、解析邮件。
"""



# 使用 `class` 定义 `EmailConfig`；该类的业务作用是：邮件协议层使用的轻量连接配置对象，只在内存中保存服务器、端口、账号、授权码、SSL 和 IMAP/POP3 协议，不映射数据库表。
class EmailConfig:
    """
        邮箱配置类
    """
    # 使用 `def` 普通函数 定义 `__init__()`。保存邮件服务器地址、端口、账号、授权码、SSL 和协议，供 EmailReader 建立连接。
    def __init__(self, host: str, port: int, username: str, password: str,
                 # 继续声明邮件连接参数：`use_ssl` 控制是否使用 SSL 加密连接，`protocol` 指定使用 IMAP 还是 POP3；行末 `):` 同时结束构造函数参数列表。
                 use_ssl: bool = True, protocol: str = "IMAP"):
        # 把 `host` 保存为实例属性 `self.host`，供同一 Agent 服务的其他方法复用；该属性表示IMAP 或 POP3 邮件服务器地址。
        self.host = host
        # 把 `port` 保存为实例属性 `self.port`，供同一 Agent 服务的其他方法复用；该属性表示邮件协议服务器端口。
        self.port = port
        # 把 `username` 保存为实例属性 `self.username`，供同一 Agent 服务的其他方法复用；该属性表示登录用户名。
        self.username = username
        # 把 `password` 保存为实例属性 `self.password`，供同一 Agent 服务的其他方法复用；该属性表示邮箱客户端授权码或用户密码。
        self.password = password
        # 把 `use_ssl` 保存为实例属性 `self.use_ssl`，供同一 Agent 服务的其他方法复用；该属性表示是否使用隐式 SSL 建立邮件连接。
        self.use_ssl = use_ssl
        # 把协议名称转换成大写后保存到 `self.protocol`，保证后续能稳定比较 `IMAP` 或 `POP3`。
        self.protocol = protocol.upper()  # IMAP or POP3


# 使用 `class` 定义 `EmailMessage`；该类的业务作用是：统一邮件消息容器，承载主题、发件人、收件人、时间、纯文本、HTML 和附件。
class EmailMessage:
    """
        邮件消息容器
    """
    # 使用 `def` 普通函数 定义 `__init__()`。把解析后的邮件头、正文和附件保存为统一对象，附件缺失时初始化为空列表。
    def __init__(self, subject: str, sender: str, recipients: List[str],
                 # 继续声明邮件解析结果字段：`date` 保存邮件时间，`body` 保存纯文本正文，`html_body` 保存可选的 HTML 正文。
                 date: datetime, body: str, html_body: Optional[str] = None,
                 # 声明局部变量 `attachments` 的类型为 `Optional[List[dict]]`，并用 `None):` 初始化；该变量用于当前邮件包含的附件列表。
                 attachments: Optional[List[dict]] = None,
                 message_id: Optional[str] = None):
        # 把 `subject # 邮件主题` 保存为实例属性 `self.subject`，供同一 Agent 服务的其他方法复用；该属性表示待发送邮件的主题。
        self.subject = subject  # 邮件主题
        # 把 `sender # 发件人` 保存为实例属性 `self.sender`，供同一 Agent 服务的其他方法复用；该属性表示用于筛选最近邮件的发件人地址。
        self.sender = sender  # 发件人
        # 把 `recipients # 收件人列表` 保存为实例属性 `self.recipients`，供同一 Agent 服务的其他方法复用；该属性表示邮件头中解析出的收件人地址列表。
        self.recipients = recipients  # 收件人列表
        # 把 `date # 邮件时间` 保存为实例属性 `self.date`，供同一 Agent 服务的其他方法复用；该属性表示邮件发送时间。
        self.date = date  # 邮件时间
        # 把 `body # 纯文本正文` 保存为实例属性 `self.body`，供同一 Agent 服务的其他方法复用；该属性表示待发送邮件的正文。
        self.body = body  # 纯文本正文
        # 把 `html_body # HTML正文` 保存为实例属性 `self.html_body`，供同一 Agent 服务的其他方法复用；该属性表示邮件的 HTML 格式正文。
        self.html_body = html_body  # HTML正文
        # 把 `attachments or [] # 附件列表` 保存为实例属性 `self.attachments`，供同一 Agent 服务的其他方法复用；该属性表示当前邮件包含的附件列表。
        self.attachments = attachments or []  # 附件列表
        # RFC Message-ID 比 IMAP 顺序号更稳定，用于跨多次定时抓取去重。
        self.message_id = message_id


# 使用 `class` 定义 `EmailReader`；该类的业务作用是：封装 IMAP/POP3 协议连接、搜索、读取、MIME 解析和连接释放的底层读取器。
class EmailReader:
    """
        核心读取类: 用于从IMAP或POP3服务器读取邮件的工具类
            拿到邮箱配置后，后续会把IMAP或POP3连接保存在self.connection
    """

    # 使用 `def` 普通函数 定义 `__init__()`。保存连接配置并把 connection 初始化为空，连接成功后再写入 IMAP/POP3 客户端。
    def __init__(self, config: EmailConfig):
        # 把 `config` 保存为实例属性 `self.config`，供同一 Agent 服务的其他方法复用；该属性表示当前数据库邮箱配置或邮件协议连接配置。
        self.config = config
        # 把 `None` 保存为实例属性 `self.connection`，供同一 Agent 服务的其他方法复用；该属性表示当前已经建立的 IMAP 或 POP3 客户端连接。
        self.connection = None

    # 使用 `def` 普通函数 定义 `connect()`。根据协议和 SSL 选择 IMAP4_SSL、IMAP4、POP3_SSL 或 POP3，使用账号与授权码登录；失败记录异常并返回 False。
    def connect(self) -> bool:
        """
            连接到邮件服务器:
                连接成功返回True，失败记录日志并返回False。
        """
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `self.config.protocol == "IMAP"`；条件成立时执行下面分支。
            if self.config.protocol == "IMAP":
                # 使用 `if` 判断 `self.config.use_ssl`；条件成立时执行下面分支。
                if self.config.use_ssl:
                    # 使用 `IMAP4_SSL` 建立加密的 IMAP 连接；当前配置通常对应 993 端口，用于安全读取收件箱。
                    self.connection = imaplib.IMAP4_SSL(self.config.host, self.config.port)
                # 前面的 `if/elif` 条件均不成立时进入该备用分支。
                else:
                    # 使用 `IMAP4` 建立非隐式 SSL 的 IMAP 连接，连接参数来自当前邮箱配置。
                    self.connection = imaplib.IMAP4(self.config.host, self.config.port)
                # 向 IMAP 服务器提交完整邮箱地址和客户端授权码完成身份认证；认证失败时 connect() 返回 False。
                self.connection.login(self.config.username, self.config.password)


            # 前一条件不成立时继续判断 `self.config.protocol == "POP3"`。
            elif self.config.protocol == "POP3":
                # 使用 `if` 判断 `self.config.use_ssl`；条件成立时执行下面分支。
                if self.config.use_ssl:
                    # 把 `poplib.POP3_SSL(self.config.host, self.config.port)` 保存为实例属性 `self.connection`，供同一 Agent 服务的其他方法复用；该属性表示当前已经建立的 IMAP 或 POP3 客户端连接。
                    self.connection = poplib.POP3_SSL(self.config.host, self.config.port)
                # 前面的 `if/elif` 条件均不成立时进入该备用分支。
                else:
                    # 把 `poplib.POP3(self.config.host, self.config.port)` 保存为实例属性 `self.connection`，供同一 Agent 服务的其他方法复用；该属性表示当前已经建立的 IMAP 或 POP3 客户端连接。
                    self.connection = poplib.POP3(self.config.host, self.config.port)
                # 调用 `self.connection.user()`，向 POP3 服务器提交邮箱账号。
                self.connection.user(self.config.username)
                # 调用 `self.connection.pass_()`，向 POP3 服务器提交客户端授权码完成认证。
                self.connection.pass_(self.config.password)
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 使用 `raise` 抛出 `ValueError(f"Unsupported protocol: {self.config.protocol}")`，立即终止当前失败流程并交给上层处理。
                raise ValueError(f"Unsupported protocol: {self.config.protocol}")

            # 通过模块日志记录器执行 `logger.info(f"Successfully connected to {self.config.protocol} server {self.config.host}")`，用于保留运行状态或异常原因。
            logger.info(f"Successfully connected to {self.config.protocol} server {self.config.host}")
            # 从 `EmailReader.connect()` 返回 `True`，并结束当前函数。
            return True
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as e:
            # 通过模块日志记录器执行 `logger.error(f"Failed to connect to email server: {str(e)}")`，用于保留运行状态或异常原因。
            logger.error(f"Failed to connect to email server: {str(e)}")
            # 从 `EmailReader.connect()` 返回 `False`，并结束当前函数。
            return False

    # 使用 `def` 普通函数 定义 `disconnect()`。按协议安全退出连接；IMAP 未选择邮箱时允许 close 失败但仍执行 logout。
    def disconnect(self):
        """
            关闭邮箱连接
        """
        # 使用 `if` 判断 `self.connection`；条件成立时执行下面分支。
        if self.connection:
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 使用 `if` 判断 `self.config.protocol == "IMAP"`；条件成立时执行下面分支。
                if self.config.protocol == "IMAP":
                    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                    try:
                        # 调用 `self.connection.close()`，关闭当前选中的 IMAP 文件夹。
                        self.connection.close()
                    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                    except Exception as close_error:
                        # 通过模块日志记录器执行 `logger.debug(f"Close command failed (likely not in SELECTED state): {str(close_error)}")`，用于保留运行状态或异常原因。
                        logger.debug(f"Close command failed (likely not in SELECTED state): {str(close_error)}")
                    # 调用 `self.connection.logout()`，退出 IMAP 会话并释放服务器连接。
                    self.connection.logout()
                # 前一条件不成立时继续判断 `self.config.protocol == "POP3"`。
                elif self.config.protocol == "POP3":
                    # 调用 `self.connection.quit()`，退出 POP3 会话并释放服务器连接。
                    self.connection.quit()
                # 通过模块日志记录器执行 `logger.info("Disconnected from email server")`，用于保留运行状态或异常原因。
                logger.info("Disconnected from email server")
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception as e:
                # 通过模块日志记录器执行 `logger.error(f"Error while disconnecting: {str(e)}")`，用于保留运行状态或异常原因。
                logger.error(f"Error while disconnecting: {str(e)}")
            # 使用 `finally` 保证无论成功或失败都执行资源清理和状态恢复。
            finally:
                # 把 `None` 保存为实例属性 `self.connection`，供同一 Agent 服务的其他方法复用；该属性表示当前已经建立的 IMAP 或 POP3 客户端连接。
                self.connection = None

    # 使用 `def` 普通函数 定义 `list_folders()`。向 IMAP 服务发送客户端 ID 后调用 list，解析并返回可用文件夹名称。
    def list_folders(self) -> List[str]:
        """
            列出邮箱文件夹（仅限IMAP）

        """
        # 使用 `if` 判断 `not self.connection or self.config.protocol != "IMAP"`；条件成立时执行下面分支。
        if not self.connection or self.config.protocol != "IMAP":
            # 从 `EmailReader.list_folders()` 返回 `[]`，并结束当前函数。
            return []

        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 为 imaplib 注册网易邮箱需要的 `ID` 扩展命令，使客户端可以在认证后发送身份信息。
            imaplib.Commands["ID"] = "NONAUTH"
            # 计算表达式 `("name", "111", "version", "1.0.0", "vendor", "myclient")` 并把结果保存到 `args`；该结果在后续流程中用作传给 IMAP 命令的参数列表。
            args = ("name", "111",  "version", "1.0.0", "vendor", "myclient")
            # 使用 Python 序列解包，把 `self.connection._simple_command('ID', '("' + '" "'.join(args) + '")')` 返回的多个结果分别赋给 `typ, dat`，供后续状态判断和数据解析。
            typ, dat = self.connection._simple_command('ID', '("' + '" "'.join(args) + '")')
            # 调用 `print()`，输出当前调试信息；生产环境更适合改用带级别的 logger。
            print(self.connection._untagged_response(typ, dat, 'ID'))

            # 使用 Python 序列解包，把 `self.connection.list()` 返回的多个结果分别赋给 `status, folders`，供后续状态判断和数据解析。
            status, folders = self.connection.list()
            # 使用 `if` 判断 `status == "OK"`；条件成立时执行下面分支。
            if status == "OK":
                # 把 `folder_names` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入IMAP 返回并解析后的邮箱文件夹名称列表。
                folder_names = []
                # 使用 `for` 循环 `folder in folders`，逐项处理集合数据。
                for folder in folders:
                    # 调用 `folder.decode('utf-8') if isinstance(folder, bytes) else folder` 完成当前解析、查询或对象构造，并把返回值保存到 `folder_str`；它表示IMAP LIST 响应中的单个文件夹描述文本。
                    folder_str = folder.decode('utf-8') if isinstance(folder, bytes) else folder
                    # 使用 `if` 判断 `'"' in folder_str`；条件成立时执行下面分支。
                    if '"' in folder_str:
                        # 调用 `folder_str.split('"')[-2]` 完成当前解析、查询或对象构造，并把返回值保存到 `folder_name`；它表示从 IMAP LIST 响应末尾提取的文件夹名称。
                        folder_name = folder_str.split('"')[-2]
                    # 前面的 `if/elif` 条件均不成立时进入该备用分支。
                    else:
                        # 调用 `folder_str.split()[-1]` 完成当前解析、查询或对象构造，并把返回值保存到 `folder_name`；它表示从 IMAP LIST 响应末尾提取的文件夹名称。
                        folder_name = folder_str.split()[-1]
                    # 调用列表的 `append()`，把 `folder_names.append(folder_name)` 对应的新元素追加到现有结果末尾。
                    folder_names.append(folder_name)
                # 从 `EmailReader.list_folders()` 返回 `folder_names`，并结束当前函数。
                return folder_names
            # 从 `EmailReader.list_folders()` 返回 `[]`，并结束当前函数。
            return []
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as e:
            # 通过模块日志记录器执行 `logger.error(f"Error listing folders: {str(e)}")`，用于保留运行状态或异常原因。
            logger.error(f"Error listing folders: {str(e)}")
            # 从 `EmailReader.list_folders()` 返回 `[]`，并结束当前函数。
            return []

    # 使用 `def` 普通函数 定义 `select_folder()`。向 IMAP 服务发送客户端 ID 并选择指定文件夹，成功返回 True。
    def select_folder(self, folder: str = "INBOX") -> int:
        """
            选择要读取邮件的文件夹（仅限IMAP）
                folder (str): 文件夹名称，默认为INBOX
                返回: 文件夹中的邮件数量
        """
        # 使用 `if` 判断 `not self.connection or self.config.protocol != "IMAP"`；条件成立时执行下面分支。
        if not self.connection or self.config.protocol != "IMAP":
            # 从 `EmailReader.select_folder()` 返回 `0`，并结束当前函数。
            return 0

        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 为 imaplib 注册网易邮箱需要的 `ID` 扩展命令，使客户端可以在认证后发送身份信息。
            imaplib.Commands["ID"] = "NONAUTH"
            # 计算表达式 `("name", "111", "version", "1.0.0", "vendor", "myclient")` 并把结果保存到 `args`；该结果在后续流程中用作传给 IMAP 命令的参数列表。
            args = ("name", "111",  "version", "1.0.0", "vendor", "myclient")
            # 使用 Python 序列解包，把 `self.connection._simple_command('ID', '("' + '" "'.join(args) + '")')` 返回的多个结果分别赋给 `typ, dat`，供后续状态判断和数据解析。
            typ, dat = self.connection._simple_command('ID', '("' + '" "'.join(args) + '")')
            # 调用 `print()`，输出当前调试信息；生产环境更适合改用带级别的 logger。
            print(self.connection._untagged_response(typ, dat, 'ID'))

            # 使用 Python 序列解包，把 `self.connection.select(folder)` 返回的多个结果分别赋给 `status, msg_count`，供后续状态判断和数据解析。
            status, msg_count = self.connection.select(folder)
            # 调用 `print()`，输出当前调试信息；生产环境更适合改用带级别的 logger。
            print(f'status:{status}, msg_count:{msg_count}')

            # 使用 `if` 判断 `status == "OK"`；条件成立时执行下面分支。
            if status == "OK":
                # 调用 `int(msg_count[0]) if isinstance(msg_count[0], bytes) else int(msg_count[0])` 完成当前解析、查询或对象构造，并把返回值保存到 `count`；它表示当前邮箱文件夹中的邮件数量。
                count = int(msg_count[0]) if isinstance(msg_count[0], bytes) else int(msg_count[0])
                # 通过模块日志记录器执行 `logger.info(f"Selected folder '{folder}' with {count} messages")`，用于保留运行状态或异常原因。
                logger.info(f"Selected folder '{folder}' with {count} messages")
                # 从 `EmailReader.select_folder()` 返回 `count`，并结束当前函数。
                return count
            # 从 `EmailReader.select_folder()` 返回 `0`，并结束当前函数。
            return 0
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as e:
            # 通过模块日志记录器执行 `logger.error(f"Error selecting folder '{folder}': {str(e)}")`，用于保留运行状态或异常原因。
            logger.error(f"Error selecting folder '{folder}': {str(e)}")
            # 从 `EmailReader.select_folder()` 返回 `0`，并结束当前函数。
            return 0

    # 使用 `def` 普通函数 定义 `search_emails()`。把搜索条件交给 IMAP search，解析空格分隔的邮件编号并转换成整数列表。
    def search_emails(self, criteria: list, charset: Optional[str] = "UTF-8") -> List[int]:
        """
            根据条件搜索邮件（仅限IMAP）
                criteria (str): 搜索条件（例如："FROM user@example.com", "SUBJECT 'test'"）
                返回：符合条件的邮件UID列表。
        """
        # 使用 `if` 判断 `not self.connection or self.config.protocol != "IMAP"`；条件成立时执行下面分支。
        if not self.connection or self.config.protocol != "IMAP":
            # 从 `EmailReader.search_emails()` 返回 `[]`，并结束当前函数。
            return []

        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 Python 序列解包，把 `self.connection.uid('SEARCH', *criteria)` 返回的多个结果分别赋给 `status, messages`，供后续状态判断和数据解析。
            status, messages = self.connection.uid('SEARCH', *criteria)
            # 使用 `if` 判断 `status == "OK"`；条件成立时执行下面分支。
            if status == "OK":
                # 计算表达式 `messages[0].split()` 并把结果保存到 `msg_ids`；该结果在后续流程中用作用于批量关联数据库记录或业务资源的唯一标识集合。
                msg_ids = messages[0].split()
                # 调用 `print()`，输出当前调试信息；生产环境更适合改用带级别的 logger。
                print(f'msg_ids:{msg_ids}')
                # 从 `EmailReader.search_emails()` 返回 `[int(msg_id) for msg_id in msg_ids]`，并结束当前函数。
                return [int(msg_id) for msg_id in msg_ids]
            # 从 `EmailReader.search_emails()` 返回 `[]`，并结束当前函数。
            return []
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as e:
            # 通过模块日志记录器执行 `logger.warning(f"Search with charset '{charset}' failed: {str(e)}; retrying without charset")`，用于保留运行状态或异常原因。
            logger.warning(f"Search with charset '{charset}' failed: {str(e)}; retrying without charset")
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 使用 Python 序列解包，把 `self.connection.search(None, *criteria)` 返回的多个结果分别赋给 `status, messages`，供后续状态判断和数据解析。
                status, messages = self.connection.search(None, *criteria)
                # 使用 `if` 判断 `status == "OK"`；条件成立时执行下面分支。
                if status == "OK":
                    # 计算表达式 `messages[0].split()` 并把结果保存到 `msg_ids`；该结果在后续流程中用作用于批量关联数据库记录或业务资源的唯一标识集合。
                    msg_ids = messages[0].split()
                    # 从 `EmailReader.search_emails()` 返回 `[int(msg_id) for msg_id in msg_ids]`，并结束当前函数。
                    return [int(msg_id) for msg_id in msg_ids]
                # 从 `EmailReader.search_emails()` 返回 `[]`，并结束当前函数。
                return []
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception as e2:
                # 通过模块日志记录器执行 `logger.error(f"Error searching emails with criteria '{criteria}': {str(e2)}")`，用于保留运行状态或异常原因。
                logger.error(f"Error searching emails with criteria '{criteria}': {str(e2)}")
                # 从 `EmailReader.search_emails()` 返回 `[]`，并结束当前函数。
                return []

    # 使用 `def` 普通函数 定义 `get_email()`。按当前协议分发到 IMAP 或 POP3 单封邮件读取方法。
    def get_email(self, msg_id: int) -> Optional[EmailMessage]:
        """
            根据邮件ID获取邮件
                msg_id (int): 邮件ID
                返回：EmailMessage邮件消息对象，失败时返回None
        """
        # 使用 `if` 判断 `not self.connection`；条件成立时执行下面分支。
        if not self.connection:
            # 从 `EmailReader.get_email()` 返回 `None`，并结束当前函数。
            return None

        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `self.config.protocol == "IMAP"`；条件成立时执行下面分支。
            if self.config.protocol == "IMAP":
                # 从 `EmailReader.get_email()` 返回 `self._get_imap_email(msg_id)`，并结束当前函数。
                return self._get_imap_email(msg_id)
            # 前一条件不成立时继续判断 `self.config.protocol == "POP3"`。
            elif self.config.protocol == "POP3":
                # 从 `EmailReader.get_email()` 返回 `self._get_pop3_email(msg_id)`，并结束当前函数。
                return self._get_pop3_email(msg_id)
            # 从 `EmailReader.get_email()` 返回 `None`，并结束当前函数。
            return None
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as e:
            # 通过模块日志记录器执行 `logger.error(f"Error retrieving email {msg_id}: {str(e)}")`，用于保留运行状态或异常原因。
            logger.error(f"Error retrieving email {msg_id}: {str(e)}")
            # 从 `EmailReader.get_email()` 返回 `None`，并结束当前函数。
            return None

    # 使用 `def` 普通函数 定义 `_get_imap_email()`。用 IMAP fetch 取得 RFC822 原始字节，解析为标准库 Message 后统一转换。
    def _get_imap_email(self, msg_id: int) -> Optional[EmailMessage]:
        """
            使用IMAP协议检索邮件
                msg_id (int): 邮件ID
                返回: EmailMessage邮件消息对象
        """
        # 使用 Python 序列解包，把 `self.connection.uid('FETCH', str(msg_id), "(RFC822)")` 返回的多个结果分别赋给 `status, msg_data`，供后续状态判断和数据解析。
        status, msg_data = self.connection.uid('FETCH', str(msg_id), "(RFC822)")
        # 使用 `if` 判断 `status != "OK"`；条件成立时执行下面分支。
        if status != "OK":
            # 从 `EmailReader._get_imap_email()` 返回 `None`，并结束当前函数。
            return None

        # 计算表达式 `msg_data[0][1]` 并把结果保存到 `raw_email`；该结果在后续流程中用作从邮件服务器读取的 RFC822 原始字节内容。
        raw_email = msg_data[0][1]
        # 调用 `email.message_from_bytes(raw_email)` 完成当前解析、查询或对象构造，并把返回值保存到 `email_message`；它表示由标准库解析后的 MIME Message 对象。
        email_message = email.message_from_bytes(raw_email)
        # 从 `EmailReader._get_imap_email()` 返回 `self._parse_email_message(email_message)`，并结束当前函数。
        return self._parse_email_message(email_message)

    # 使用 `def` 普通函数 定义 `_get_pop3_email()`。用 POP3 retr 取得邮件各行，合并成原始字节后统一解析。
    def _get_pop3_email(self, msg_id: int) -> Optional[EmailMessage]:
        """
            使用POP3协议检索邮件
                msg_id (int): 邮件ID（基于1的索引）
                返回: EmailMessage邮件消息对象
        """
        # 使用 Python 序列解包，把 `self.connection.retr(msg_id)` 返回的多个结果分别赋给 `msg_lines, octets`，供后续状态判断和数据解析。
        msg_lines, octets = self.connection.retr(msg_id)
        # 计算表达式 `b'\n'.join(msg_lines)` 并把结果保存到 `raw_email`；该结果在后续流程中用作从邮件服务器读取的 RFC822 原始字节内容。
        raw_email = b'\n'.join(msg_lines)
        # 调用 `email.message_from_bytes(raw_email)` 完成当前解析、查询或对象构造，并把返回值保存到 `email_message`；它表示由标准库解析后的 MIME Message 对象。
        email_message = email.message_from_bytes(raw_email)
        # 从 `EmailReader._get_pop3_email()` 返回 `self._parse_email_message(email_message)`，并结束当前函数。
        return self._parse_email_message(email_message)

    # 使用 `def` 普通函数 定义 `_parse_email_message()`。解析 MIME 邮件头和 multipart 内容，区分纯文本、HTML 与附件并返回 EmailMessage。
    def _parse_email_message(self, email_message: Message) -> EmailMessage:
        """
            解析邮件内容
                email_message (Message): 邮件消息
        """
        # 调用 `self._decode_header_value(email_message.get("Subject", ""))` 完成当前解析、查询或对象构造，并把返回值保存到 `subject`；它表示待发送邮件的主题。
        subject = self._decode_header_value(email_message.get("Subject", ""))
        # 读取邮件标准 Message-ID；若邮件没有该字段，上层会退回使用 IMAP 邮件编号。
        message_id = self._decode_header_value(email_message.get("Message-ID", ""))

        # 调用 `self._decode_header_value(email_message.get("From", ""))` 完成当前解析、查询或对象构造，并把返回值保存到 `sender`；它表示用于筛选最近邮件的发件人地址。
        sender = self._decode_header_value(email_message.get("From", ""))

        # 调用 `email_message.get("To", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `to_header`；它表示原始 To 邮件头，用于解析全部收件人。
        to_header = email_message.get("To", "")
        # 计算表达式 `[self._decode_header_value(addr) for addr in to_header.split(",")] if to_header else []` 并把结果保存到 `recipients`；该结果在后续流程中用作邮件头中解析出的收件人地址列表。
        recipients = [self._decode_header_value(addr) for addr in to_header.split(",")] if to_header else []

        # 调用 `email_message.get("Date", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `date_str`；它表示原始 Date 邮件头字符串。
        date_str = email_message.get("Date", "")
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 调用 `datetime.strptime(date_str[:-6], "%a, %d %b %Y %H:%M:%S") if date_str else datetime.now()` 完成当前解析、查询或对象构造，并把返回值保存到 `date`；它表示邮件发送时间。
            date = datetime.strptime(date_str[:-6], "%a, %d %b %Y %H:%M:%S") if date_str else datetime.now()
        # 使用不带异常类型的 `except` 捕获邮件日期解析中的任意异常；解析失败后改用当前时间，避免整封邮件处理失败。
        except:
            # 调用 `datetime.now()` 完成当前解析、查询或对象构造，并把返回值保存到 `date`；它表示邮件发送时间。
            date = datetime.now()

        # 把 `body` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入待发送邮件的正文。
        body = ""
        # 把 `html_body` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入邮件的 HTML 格式正文。
        html_body = ""
        # 把 `attachments` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入当前邮件包含的附件列表。
        attachments = []

        # 使用 `if` 判断 `email_message.is_multipart()`；条件成立时执行下面分支。
        if email_message.is_multipart():
            # 使用 `for` 循环 `part in email_message.walk()`，逐项处理集合数据。
            for part in email_message.walk():
                # 调用 `part.get_content_type()` 完成当前解析、查询或对象构造，并把返回值保存到 `content_type`；它表示附件 MIME 类型，供后端判断 PDF、DOCX 等文件格式。
                content_type = part.get_content_type()
                # 调用 `str(part.get("Content-Disposition", ""))` 完成当前解析、查询或对象构造，并把返回值保存到 `content_disposition`；它表示当前 MIME 部分的内容处置类型，用于区分正文和附件。
                content_disposition = str(part.get("Content-Disposition", ""))
                attachment_filename = part.get_filename()

                # 兼容 attachment、inline 以及不同大小写：只要 MIME 部分带文件名，就按附件处理。
                if "attachment" in content_disposition.lower() or attachment_filename:
                    # 调用 `part.get_filename()` 完成当前解析、查询或对象构造，并把返回值保存到 `filename`；它表示邮件附件原始文件名。
                    filename = attachment_filename
                    # 使用 `if` 判断 `filename`；条件成立时执行下面分支。
                    if filename:
                        # 调用 `self._decode_header_value(filename)` 完成当前解析、查询或对象构造，并把返回值保存到 `filename`；它表示邮件附件原始文件名。
                        filename = self._decode_header_value(filename)
                        # 调用 `part.get_payload(decode=True)` 完成当前解析、查询或对象构造，并把返回值保存到 `payload`；它表示本次 Skill 或接口准备交给下游服务的结构化业务数据。
                        payload = part.get_payload(decode=True)
                        # 调用列表的 `append()`，把 `attachments.append({` 对应的新元素追加到现有结果末尾。
                        attachments.append({
                            # 在字典中设置 `filename` 字段为 `filename`，用于表示邮件附件原始文件名。
                            "filename": filename,
                            # 在字典中设置 `content` 字段为 `payload`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                            "content": payload,
                            # 在字典中设置 `content_type` 字段为 `content_type`，用于表示附件 MIME 类型，供后端判断 PDF、DOCX 等文件格式。
                            "content_type": content_type
                        # 结束 `EmailReader._parse_email_message` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        })
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue

                # 使用 `if` 判断 `content_type == "text/plain"`；条件成立时执行下面分支。
                if content_type == "text/plain":
                    # 调用 `part.get_content_charset() or "utf-8"` 完成当前解析、查询或对象构造，并把返回值保存到 `charset`；它表示邮件头或 MIME 正文声明的字符编码。
                    charset = part.get_content_charset() or "utf-8"
                    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                    try:
                        # 调用 `part.get_payload(decode=True).decode(charset, errors="ignore")` 完成当前解析、查询或对象构造，并把返回值保存到 `body`；它表示待发送邮件的正文。
                        body = part.get_payload(decode=True).decode(charset, errors="ignore")
                    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                    except Exception:
                        # 调用 `part.get_payload()` 完成当前解析、查询或对象构造，并把返回值保存到 `body`；它表示待发送邮件的正文。
                        body = part.get_payload()

                # 前一条件不成立时继续判断 `content_type == "text/html"`。
                elif content_type == "text/html":
                    # 调用 `part.get_content_charset() or "utf-8"` 完成当前解析、查询或对象构造，并把返回值保存到 `charset`；它表示邮件头或 MIME 正文声明的字符编码。
                    charset = part.get_content_charset() or "utf-8"
                    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                    try:
                        # 调用 `part.get_payload(decode=True).decode(charset, errors="ignore")` 完成当前解析、查询或对象构造，并把返回值保存到 `html_body`；它表示邮件的 HTML 格式正文。
                        html_body = part.get_payload(decode=True).decode(charset, errors="ignore")
                    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                    except Exception:
                        # 调用 `part.get_payload()` 完成当前解析、查询或对象构造，并把返回值保存到 `html_body`；它表示邮件的 HTML 格式正文。
                        html_body = part.get_payload()

        # 前面的 `if/elif` 条件均不成立时进入该备用分支。
        else:
            # 调用 `email_message.get_content_type()` 完成当前解析、查询或对象构造，并把返回值保存到 `content_type`；它表示附件 MIME 类型，供后端判断 PDF、DOCX 等文件格式。
            content_type = email_message.get_content_type()
            # 调用 `email_message.get_content_charset() or "utf-8"` 完成当前解析、查询或对象构造，并把返回值保存到 `charset`；它表示邮件头或 MIME 正文声明的字符编码。
            charset = email_message.get_content_charset() or "utf-8"
            # 使用 `if` 判断 `content_type == "text/plain"`；条件成立时执行下面分支。
            if content_type == "text/plain":
                # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                try:
                    # 调用 `email_message.get_payload(decode=True).decode(charset, errors="ignore")` 完成当前解析、查询或对象构造，并把返回值保存到 `body`；它表示待发送邮件的正文。
                    body = email_message.get_payload(decode=True).decode(charset, errors="ignore")
                # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                except Exception:
                    # 调用 `email_message.get_payload()` 完成当前解析、查询或对象构造，并把返回值保存到 `body`；它表示待发送邮件的正文。
                    body = email_message.get_payload()
            # 前一条件不成立时继续判断 `content_type == "text/html"`。
            elif content_type == "text/html":
                # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                try:
                    # 调用 `email_message.get_payload(decode=True).decode(charset, errors="ignore")` 完成当前解析、查询或对象构造，并把返回值保存到 `html_body`；它表示邮件的 HTML 格式正文。
                    html_body = email_message.get_payload(decode=True).decode(charset, errors="ignore")
                # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                except Exception:
                    # 调用 `email_message.get_payload()` 完成当前解析、查询或对象构造，并把返回值保存到 `html_body`；它表示邮件的 HTML 格式正文。
                    html_body = email_message.get_payload()

        # 从 `EmailReader._parse_email_message()` 返回 `EmailMessage(`，并结束当前函数。
        return EmailMessage(
            # 使用关键字参数语法，把 `subject` 传入 `subject` 参数；它表示待发送邮件的主题。
            subject=subject,
            # 使用关键字参数语法，把 `sender` 传入 `sender` 参数；它表示用于筛选最近邮件的发件人地址。
            sender=sender,
            # 使用关键字参数语法，把 `recipients` 传入 `recipients` 参数；它表示邮件头中解析出的收件人地址列表。
            recipients=recipients,
            # 使用关键字参数语法，把 `date` 传入 `date` 参数；它表示邮件发送时间。
            date=date,
            # 使用关键字参数语法，把 `body` 传入 `body` 参数；它表示待发送邮件的正文。
            body=body,
            # 使用关键字参数语法，把 `html_body` 传入 `html_body` 参数；它表示邮件的 HTML 格式正文。
            html_body=html_body,
            # 计算表达式 `attachments` 并把结果保存到 `attachments`；该结果在后续流程中用作当前邮件包含的附件列表。
            attachments=attachments,
            message_id=message_id,
        # 结束 `EmailReader._parse_email_message` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_decode_header_value()`。解析可能采用 Base64/Quoted-Printable 和不同字符集编码的邮件头，拼接成正常字符串。
    def _decode_header_value(self, header_value: str) -> str:
        """
            解码邮件头：用来处理邮件头里的编码问题
                header_value: Encoded header value
        """
        # 使用 `if` 判断 `not header_value`；条件成立时执行下面分支。
        if not header_value:
            # 从 `EmailReader._decode_header_value()` 返回 `""`，并结束当前函数。
            return ""

        # 调用 `decode_header(header_value)` 完成当前解析、查询或对象构造，并把返回值保存到 `decoded_parts`；它表示同一个邮件头按字符集解码后的文本片段。
        decoded_parts = decode_header(header_value)
        # 把 `decoded_string` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入全部邮件头片段拼接得到的可读字符串。
        decoded_string = ""
        # 使用 `for` 循环 `part, encoding in decoded_parts`，逐项处理集合数据。
        for part, encoding in decoded_parts:
            # 使用 `if` 判断 `isinstance(part, bytes)`；条件成立时执行下面分支。
            if isinstance(part, bytes):
                # 使用 `if` 判断 `encoding`；条件成立时执行下面分支。
                if encoding:
                    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                    try:
                        # 使用 `+=` 将 `part.decode(encoding)` 累加到 `decoded_string`；业务作用是把当前已解码的邮件头片段追加到完整结果。
                        decoded_string += part.decode(encoding)
                    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                    except Exception:
                        # 使用 `+=` 将 `part.decode('utf-8', errors='ignore')` 累加到 `decoded_string`；业务作用是把当前已解码的邮件头片段追加到完整结果。
                        decoded_string += part.decode('utf-8', errors='ignore')
                # 前面的 `if/elif` 条件均不成立时进入该备用分支。
                else:
                    # 使用 `+=` 将 `part.decode('utf-8', errors='ignore')` 累加到 `decoded_string`；业务作用是把当前已解码的邮件头片段追加到完整结果。
                    decoded_string += part.decode('utf-8', errors='ignore')
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 使用 `+=` 将 `part` 累加到 `decoded_string`；业务作用是把当前已解码的邮件头片段追加到完整结果。
                decoded_string += part
        # 从 `EmailReader._decode_header_value()` 返回 `decoded_string`，并结束当前函数。
        return decoded_string

    # 使用 `def` 普通函数 定义 `__enter__()`。支持 with 语法：进入上下文时连接邮箱，失败则抛出 ConnectionError。
    def __enter__(self):
        """ 进入with时自动连接 """
        # 调用 `self.connect()`，进入 with 上下文时建立并认证邮箱连接。
        self.connect()
        # 从 `EmailReader.__enter__()` 返回 `self`，并结束当前函数。
        return self

    # 使用 `def` 普通函数 定义 `__exit__()`。退出 with 代码块时自动断开邮箱连接，避免连接泄漏。
    def __exit__(self, exc_type, exc_val, exc_tb):
        """ 退出时自动断开连接 """
        # 调用 `self.disconnect()`，退出 with 上下文时自动释放邮箱连接。
        self.disconnect()


# 使用 `if` 判断 `__name__ == "__main__"`；条件成立时执行下面分支。
