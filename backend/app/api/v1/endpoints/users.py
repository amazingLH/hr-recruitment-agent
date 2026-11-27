"""
* 文件作用
  * 这是系统用户与角色管理的 FastAPI 接口层。它提供用户列表、详情、修改、删除、管理员创建用户、查询/分配角色以及角色增删接口；接口通过 get_current_superuser 限制后台管理操作，再调用 UserService 和 RoleService 完成数据库业务。
* 类说明
  * 本文件没有定义业务类，使用模块级函数实现 FastAPI 接口或 Skill 阶段。
* 主要函数与方法说明
  * `get_users()`：超级管理员分页查询全部用户，并为每个用户追加 RoleService 查询到的角色列表后返回。
  * `get_user()`：按 user_id 查询单个用户；不存在返回 404，非超级管理员只能查看自己的资料。
  * `update_user()`：校验访问权限后把 UserUpdate 中实际提供的字段交给 UserService 更新；普通用户只能修改自己。
  * `delete_user()`：仅超级管理员可按 user_id 删除用户；目标不存在返回 404，删除结果由 UserService 提交。
  * `admin_create_user()`：仅超级管理员创建用户；校验用户名和邮箱唯一性、哈希密码，并可为新用户分配默认角色。
  * `admin_list_user_roles()`：仅超级管理员查询指定用户当前关联的全部 Role。
  * `admin_assign_user_roles()`：仅超级管理员用 role_ids 替换指定用户的角色关联，RoleService 校验用户和角色后重建 user_role 记录。
  * `admin_list_roles()`：仅超级管理员列出系统全部角色。
  * `admin_create_role()`：仅超级管理员创建角色，名称重复时由服务层拒绝。
  * `admin_delete_role()`：仅超级管理员删除非内置角色，同时依赖关联表级联清理用户角色关系。
  * `get_my_roles()`：返回当前登录用户自己的角色列表，不要求超级管理员权限。
"""

# 导入 Any, List, Optional，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import Any, List, Optional
# 导入 UUID，用于使用 UUID 标识用户、会话和业务资源。
from uuid import UUID
# 导入 APIRouter, Depends, HTTPException, status，用于FastAPI 路由、依赖注入和 HTTP 异常能力。
from fastapi import APIRouter, Depends, HTTPException, status
# 导入 AsyncSession，用于异步数据库会话。
from sqlalchemy.ext.asyncio import AsyncSession

# 导入 get_db，用于取得 FastAPI 请求范围内的异步数据库会话。
from app.core.database import get_db
# 导入 User as UserSchema, UserUpdate, UserCreate, Role as RoleSchema, RoleCreate, AssignRolesRequest, UserWithRoles，用于当前登录用户响应模型。
from app.schemas.user import User as UserSchema, UserUpdate, UserCreate, Role as RoleSchema, RoleCreate, AssignRolesRequest, UserWithRoles
# 导入 UserService, RoleService，用于调用 UserService, RoleService 对应的业务服务。
from app.services.user_service import UserService, RoleService
# 导入 get_current_user, get_current_admin_by_role，用于解析 JWT 并取得当前登录用户。
from app.api.deps import get_current_user, get_current_admin_by_role
# 导入 UserRole，用于查询或保存 UserRole 对应的 ORM 数据。
from app.models.user import UserRole

# 创建本模块的 FastAPI `APIRouter`，下方装饰器会把用户或邮箱管理接口注册到该路由器。
router = APIRouter()


# 将 GET / 注册到下方处理函数。
@router.get("/", response_model=List[UserWithRoles])
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `get_users()`。超级管理员分页查询全部用户，并为每个用户追加 RoleService 查询到的角色列表后返回。
async def get_users(
    # 声明局部变量 `skip` 的类型为 `int`，并用 `0,` 初始化；该变量用于分页时跳过的记录数量。
    skip: int = 0,
    # 声明局部变量 `limit` 的类型为 `int`，并用 `100,` 初始化；该变量用于本次查询或抓取允许返回的最大数量。
    limit: int = 100,
    # 声明局部变量 `search` 的类型为 `Optional[str]`，并用 `None,` 初始化；该变量用于管理员输入的用户搜索关键词。
    search: Optional[str] = None,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    获取所有用户（仅管理员）
            get_users 支持分页和搜索，搜索时走UserService.search_users，否则走 get_users_with_roles。
    """
    # 调用 `UserService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `user_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    user_service = UserService(db)

    # 使用 `if` 判断 `search`；条件成立时执行下面分支。
    if search:
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `await` 等待 `user_service.search_users(search, current_user, limit)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `search_results`；该结果用于当前查询或批处理得到的结果集合。
            search_results = await user_service.search_users(search, current_user, limit)
            # 把 `result` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入当前查询、解析或业务调用产生的结果。
            result = []
            # 使用 `for` 循环 `user in search_results`，逐项处理集合数据。
            for user in search_results:
                # 调用 `RoleService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `role_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
                role_service = RoleService(db)
                # 使用 `await` 等待 `role_service.list_user_roles(user.id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `user_roles`；该结果用于指定用户通过 user_role 关联的角色列表。
                user_roles = await role_service.list_user_roles(user.id)

                # 使用字典字面量开始构造 `user_data`；该字典用于保存合并用户基本字段和角色列表后的 API 响应字典，下面各行会继续写入具体业务字段。
                user_data = {
                    # 在字典中设置 `id` 字段为 `user.id`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": user.id,
                    # 在字典中设置 `username` 字段为 `user.username`，用于表示登录用户名。
                    "username": user.username,
                    # 在字典中设置 `email` 字段为 `user.email`，用于表示邮箱账号或用户联系邮箱。
                    "email": user.email,
                    # 在字典中设置 `full_name` 字段为 `user.full_name`，用于表示用户真实姓名。
                    "full_name": user.full_name,
                    # 在字典中设置 `phone` 字段为 `user.phone`，用于表示用户手机号。
                    "phone": user.phone,
                    # 在字典中设置 `department` 字段为 `user.department`，用于表示用户所属部门。
                    "department": user.department,
                    # 在字典中设置 `position` 字段为 `user.position`，用于表示用户职位。
                    "position": user.position,
                    # 在字典中设置 `employee_id` 字段为 `user.employee_id`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                    "employee_id": user.employee_id,
                    # 在字典中设置 `role` 字段为 `user.role`，用于表示用户基础角色枚举或当前邮件消息角色。
                    "role": user.role,
                    # 在字典中设置 `is_superuser` 字段为 `user.is_superuser`，用于表示是否拥有系统管理最高权限。
                    "is_superuser": user.is_superuser,
                    # 在字典中设置 `is_verified` 字段为 `user.is_verified`，用于表示用户账号是否已经验证。
                    "is_verified": user.is_verified,
                    # 在字典中设置 `is_active` 字段为 `user.is_active`，用于表示数据库记录是否处于可用状态。
                    "is_active": user.is_active,
                    # 在字典中设置 `bio` 字段为 `user.bio`，用于表示用户个人简介。
                    "bio": user.bio,
                    # 在字典中设置 `avatar_url` 字段为 `user.avatar_url`，用于表示用户头像地址。
                    "avatar_url": user.avatar_url,
                    # 在字典中设置 `last_login` 字段为 `user.last_login`，用于表示用户最后登录时间。
                    "last_login": user.last_login,
                    # 在字典中设置 `created_at` 字段为 `user.created_at`，用于表示数据库记录创建时间。
                    "created_at": user.created_at,
                    # 在字典中设置 `updated_at` 字段为 `user.updated_at`，用于表示数据库记录最后更新时间。
                    "updated_at": user.updated_at,
                    # 在字典中设置 `roles` 字段为 `[`，用于表示用户当前绑定的 Role 列表。
                    "roles": [
                        # 使用字典字面量语法开始构造 `get_users` 当前步骤所需的键值数据。
                        {
                            # 在字典中设置 `id` 字段为 `r.id`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                            "id": r.id,
                            # 把 Role ORM 对象的名称写入响应字典的 `name` 字段，供前端显示该用户拥有的角色。
                            "name": r.name,
                            # 在字典中设置 `description` 字段为 `r.description`，用于表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                            "description": r.description,
                            # 在字典中设置 `is_builtin` 字段为 `r.is_builtin`，用于表示是否为系统内置、不可随意删除的角色。
                            "is_builtin": r.is_builtin,
                            # 在字典中设置 `created_at` 字段为 `r.created_at`，用于表示数据库记录创建时间。
                            "created_at": r.created_at,
                            # 在字典中设置 `updated_at` 字段为 `r.updated_at`，用于表示数据库记录最后更新时间。
                            "updated_at": r.updated_at,
                        # 结束 `get_users` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        }
                        # 使用 `for` 循环 `r in user_roles`，逐项处理集合数据。
                        for r in user_roles
                    # 结束 `get_users` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                # 结束 `get_users` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
                # 调用列表的 `append()`，把 `result.append(user_data)` 对应的新元素追加到现有结果末尾。
                result.append(user_data)
            # 从 `get_users()` 返回 `result`，并结束当前函数。
            return result
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except PermissionError:
            # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
            raise HTTPException(
                # 使用关键字参数语法，把 `status.HTTP_403_FORBIDDEN` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
                status_code=status.HTTP_403_FORBIDDEN,
                # 计算表达式 `"权限不足"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                detail="权限不足"
            # 结束 `get_users` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
    # 前面的 `if/elif` 条件均不成立时进入该备用分支。
    else:
        # 使用 `await` 等待 `user_service.get_users_with_roles(skip=skip, limit=limit)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `result`；该结果用于当前查询、解析或业务调用产生的结果。
        result = await user_service.get_users_with_roles(skip=skip, limit=limit)
        # 从 `get_users()` 返回 `result`，并结束当前函数。
        return result


# 将 GET /{user_id} 注册到下方处理函数。
@router.get("/{user_id}", response_model=UserSchema)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `get_user()`。按 user_id 查询单个用户；不存在返回 404，非超级管理员只能查看自己的资料。
async def get_user(
    # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `str`。
    user_id: str,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_user),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    根据ID获取用户
    """
    # 调用 `UserService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `user_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    user_service = UserService(db)
    # 使用 `await` 等待 `user_service.get_user(UUID(user_id), include_inactive=True)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `user`；该结果用于当前查询、修改或删除的 User ORM 记录。
    user = await user_service.get_user(UUID(user_id), include_inactive=True)

    # 使用 `if` 判断 `not user`；条件成立时执行下面分支。
    if not user:
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_404_NOT_FOUND` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
            status_code=status.HTTP_404_NOT_FOUND,
            # 计算表达式 `"用户未找到"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="用户未找到"
        # 结束 `get_user` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `if` 判断 `not user_service.can_view_user(current_user, user.id)`；条件成立时执行下面分支。
    if not user_service.can_view_user(current_user, user.id):
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_403_FORBIDDEN` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
            status_code=status.HTTP_403_FORBIDDEN,
            # 计算表达式 `"权限不足"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="权限不足"
        # 结束 `get_user` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 从 `get_user()` 返回 `user`，并结束当前函数。
    return user


# 将 PUT /{user_id} 注册到下方处理函数。
@router.put("/{user_id}", response_model=UserSchema)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `update_user()`。校验访问权限后把 UserUpdate 中实际提供的字段交给 UserService 更新；普通用户只能修改自己。
async def update_user(
    # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `str`。
    user_id: str,
    # 声明参数 `user_update`，表示前端提交并通过 Pydantic 校验的用户更新字段，类型标注为 `UserUpdate`。
    user_update: UserUpdate,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_user),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    更新用户信息
    """
    # 调用 `UserService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `user_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    user_service = UserService(db)
    # 使用 `await` 等待 `user_service.get_user(UUID(user_id), include_inactive=True)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `user`；该结果用于当前查询、修改或删除的 User ORM 记录。
    user = await user_service.get_user(UUID(user_id), include_inactive=True)

    # 使用 `if` 判断 `not user`；条件成立时执行下面分支。
    if not user:
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_404_NOT_FOUND` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
            status_code=status.HTTP_404_NOT_FOUND,
            # 计算表达式 `"用户未找到"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="用户未找到"
        # 结束 `update_user` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `if` 判断 `not user_service.can_update_user(current_user, user.id)`；条件成立时执行下面分支。
    if not user_service.can_update_user(current_user, user.id):
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_403_FORBIDDEN` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
            status_code=status.HTTP_403_FORBIDDEN,
            # 计算表达式 `"权限不足"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="权限不足"
        # 结束 `update_user` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `await` 等待 `user_service.update_user(user.id, user_update, current_user)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `updated_user`；该结果用于数据库提交后刷新得到的最新用户记录。
    updated_user = await user_service.update_user(user.id, user_update, current_user)
    # 从 `update_user()` 返回 `updated_user`，并结束当前函数。
    return updated_user


# 将 DELETE /{user_id} 注册到下方处理函数。
@router.delete("/{user_id}")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `delete_user()`。仅超级管理员可按 user_id 删除用户；目标不存在返回 404，删除结果由 UserService 提交。
async def delete_user(
    # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `str`。
    user_id: str,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    删除用户（仅管理员）
    """
    # 调用 `UserService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `user_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    user_service = UserService(db)
    # 使用 `await` 等待 `user_service.get_user(UUID(user_id))` 的异步数据库、邮箱或业务操作完成，把结果保存到 `user`；该结果用于当前查询、修改或删除的 User ORM 记录。
    user = await user_service.get_user(UUID(user_id))

    # 使用 `if` 判断 `not user`；条件成立时执行下面分支。
    if not user:
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_404_NOT_FOUND` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
            status_code=status.HTTP_404_NOT_FOUND,
            # 计算表达式 `"用户未找到"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="用户未找到"
        # 结束 `delete_user` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `if` 判断 `not user_service.can_delete_user(current_user, user.id)`；条件成立时执行下面分支。
    if not user_service.can_delete_user(current_user, user.id):
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_403_FORBIDDEN` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
            status_code=status.HTTP_403_FORBIDDEN,
            # 计算表达式 `"权限不足"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="权限不足"
        # 结束 `delete_user` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `await` 等待 `user_service.delete_user(user.id, current_user)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `result`；该结果用于当前查询、解析或业务调用产生的结果。
    result = await user_service.delete_user(user.id, current_user)
    # 使用 `if` 判断 `not result`；条件成立时执行下面分支。
    if not result:
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_400_BAD_REQUEST` 传入 `status_code` 参数；它表示准备返回给前端的 HTTP 状态码。
            status_code=status.HTTP_400_BAD_REQUEST,
            # 计算表达式 `"删除用户失败"` 并把结果保存到 `detail`；该结果在后续流程中用作步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="删除用户失败"
        # 结束 `delete_user` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 从 `delete_user()` 返回 `{"message": "用户删除成功"}`，并结束当前函数。
    return {"message": "用户删除成功"}




# 将 POST /admin/users 注册到下方处理函数。
@router.post("/admin/users", response_model=UserSchema)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `admin_create_user()`。仅超级管理员创建用户；校验用户名和邮箱唯一性、哈希密码，并可为新用户分配默认角色。
async def admin_create_user(
    # 声明参数 `user_create`，表示管理员提交并通过 Pydantic 校验的新用户数据，类型标注为 `UserCreate`。
    user_create: UserCreate,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    管理员创建用户
    """
    # 调用 `UserService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `user_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    user_service = UserService(db)
    # 使用 `await` 等待 `user_service.create_user(user_create)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `user`；该结果用于当前查询、修改或删除的 User ORM 记录。
    user = await user_service.create_user(user_create)
    # 从 `admin_create_user()` 返回 `user`，并结束当前函数。
    return user


# 将 GET /admin/users/{user_id}/roles 注册到下方处理函数。
@router.get("/admin/users/{user_id}/roles", response_model=List[RoleSchema])
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `admin_list_user_roles()`。仅超级管理员查询指定用户当前关联的全部 Role。
async def admin_list_user_roles(
    # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `str`。
    user_id: str,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    管理员列出用户角色
    """
    # 调用 `RoleService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `role_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    role_service = RoleService(db)
    # 使用 `await` 等待 `role_service.list_user_roles(UUID(user_id))` 的异步数据库、邮箱或业务操作完成，把结果保存到 `roles`；该结果用于用户当前绑定的 Role 列表。
    roles = await role_service.list_user_roles(UUID(user_id))
    # 从 `admin_list_user_roles()` 返回 `roles`，并结束当前函数。
    return roles


# 将 PUT /admin/users/{user_id}/roles 注册到下方处理函数。
@router.put("/admin/users/{user_id}/roles", response_model=List[RoleSchema])
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `admin_assign_user_roles()`。仅超级管理员用 role_ids 替换指定用户的角色关联，RoleService 校验用户和角色后重建 user_role 记录。
async def admin_assign_user_roles(
    # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `str`。
    user_id: str,
    # 声明参数 `payload`，表示本次 Skill 或接口准备交给下游服务的结构化业务数据，类型标注为 `AssignRolesRequest`。
    payload: AssignRolesRequest,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    管理员分配用户角色
    """
    # 调用 `RoleService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `role_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    role_service = RoleService(db)
    # 使用 `await` 等待 `role_service.assign_roles_to_user(UUID(user_id), payload.role_ids)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `roles`；该结果用于用户当前绑定的 Role 列表。
    roles = await role_service.assign_roles_to_user(UUID(user_id), payload.role_ids)
    # 从 `admin_assign_user_roles()` 返回 `roles`，并结束当前函数。
    return roles



# 将 GET /admin/roles 注册到下方处理函数。
@router.get("/admin/roles", response_model=List[RoleSchema])
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `admin_list_roles()`。仅超级管理员列出系统全部角色。
async def admin_list_roles(
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    管理员列出所有角色
    """
    # 调用 `RoleService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `role_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    role_service = RoleService(db)
    # 从 `admin_list_roles()` 返回 `await role_service.list_roles()`，并结束当前函数。
    return await role_service.list_roles()


# 将 POST /admin/roles 注册到下方处理函数。
@router.post("/admin/roles", response_model=RoleSchema)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `admin_create_role()`。仅超级管理员创建角色，名称重复时由服务层拒绝。
async def admin_create_role(
    # 声明参数 `role_create`，表示管理员提交并通过 Pydantic 校验的新角色数据，类型标注为 `RoleCreate`。
    role_create: RoleCreate,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    管理员创建角色
    """
    # 调用 `RoleService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `role_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    role_service = RoleService(db)
    # 从 `admin_create_role()` 返回 `await role_service.create_role(role_create.name, role_create.description, role_create.is_builtin ...`，并结束当前函数。
    return await role_service.create_role(role_create.name, role_create.description, role_create.is_builtin or False)


# 将 DELETE /admin/roles/{role_id} 注册到下方处理函数。
@router.delete("/admin/roles/{role_id}")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `admin_delete_role()`。仅超级管理员删除非内置角色，同时依赖关联表级联清理用户角色关系。
async def admin_delete_role(
    # 声明参数 `role_id`，表示用于关联对应数据库记录或业务资源的唯一标识，类型标注为 `str`。
    role_id: str,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_admin_by_role),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    管理员删除角色
    """
    # 调用 `RoleService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `role_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    role_service = RoleService(db)
    # 使用 `await` 等待 `role_service.delete_role(UUID(role_id))` 的异步数据库、邮箱或业务操作完成，把结果保存到 `ok`；该结果用于当前连接、删除或业务操作是否成功的布尔结果。
    ok = await role_service.delete_role(UUID(role_id))
    # 使用 `if` 判断 `not ok`；条件成立时执行下面分支。
    if not ok:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="角色未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="角色未找到")
    # 从 `admin_delete_role()` 返回 `{"message": "角色删除成功"}`，并结束当前函数。
    return {"message": "角色删除成功"}

# 将 GET /me/roles 注册到下方处理函数。
@router.get("/me/roles", response_model=List[RoleSchema])
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `get_my_roles()`。返回当前登录用户自己的角色列表，不要求超级管理员权限。
async def get_my_roles(
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_user),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db)` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    获取当前用户的角色
    """
    # 调用 `RoleService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `role_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
    role_service = RoleService(db)
    # 从 `get_my_roles()` 返回 `await role_service.list_user_roles(current_user.id)`，并结束当前函数。
    return await role_service.list_user_roles(current_user.id)
