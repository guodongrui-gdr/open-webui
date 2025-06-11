# -*- coding: utf-8 -*-
"""
@ Desc:
@ Date: 2025/4/3
@ Author: gdr
"""
import logging
import sys
from io import BytesIO
from typing import Tuple, List, BinaryIO

import requests
from fastapi import HTTPException

from open_webui.config import FEISHU_APP_ID, FEISHU_APP_SECRET
from open_webui.env import SRC_LOG_LEVELS, GLOBAL_LOG_LEVEL

logging.basicConfig(stream=sys.stdout, level=GLOBAL_LOG_LEVEL)
log = logging.getLogger(__name__)
log.setLevel(SRC_LOG_LEVELS["OAUTH"])


def get_tenant_access_token() -> str:
    """
    获取 tenant_access_token
    """
    headers = {
        'Content-Type': 'application/json'
    }
    # 获取token
    data = {
        "app_id": FEISHU_APP_ID.value,
        "app_secret": FEISHU_APP_SECRET.value
    }
    response = requests.post(f"https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal", data, headers)
    tenant_access_token = response.json()['tenant_access_token']
    return tenant_access_token


def extract_doc_info(url: str) -> Tuple[str, str]:
    """
    从飞书文档URL中提取文档token和文档类型
    """
    # 示例URL格式：https://jqx28l0j4lx.feishu.cn/docx/IWGqd3Ww6ob9vlxdcXkcMBESngg
    parts = url.split("/")
    print(parts)
    doc_type = parts[-2]
    doc_token = parts[-1]
    if "?" in doc_token:
        doc_token = doc_token.split('?')[0]
    if doc_type == 'wiki': # 调用获取知识空间节点信息接口获取文档token和文档类型
        return get_wiki_info(doc_token)
    elif doc_type == 'base': # 文档类型为多维表格
        doc_type = 'bitable'
        doc_token = doc_token.split('?')[0]
    return doc_token, doc_type


def get_wiki_info(token: str, tenant_access_token: str = None) -> Tuple[str, str]:
    """
    获取知识空间节点信息
    Args:
        token: 知识库的节点 token
        tenant_access_token: 应用token

    Returns: 文档token, 文档类型

    """
    tenant_access_token = tenant_access_token or get_tenant_access_token()
    url = 'https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node'
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    params = {
        "token": token
    }
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data['code'] == 0:
            return data['data']["node"]['obj_token'], data['data']["node"]['obj_type']
        elif data["code"] == 131006: # 无权限
            raise HTTPException(401, detail="文档未授权,请将文档链接分享范围设置为凡岛")
        else:
            log.error(f"获取知识空间节点信息失败: {data['msg']}")
            raise HTTPException(500, detail=data['msg'])
    else:
        log.error(f"获取知识空间节点信息失败: {response.text}")
        raise HTTPException(500, detail=response.text)


def get_sheet_token(spreadsheet_token: str, tenant_access_token: str = None) -> List[str]:
    """
    获取表格ID
    Args:
        spreadsheet_token: 电子表格的 token
        tenant_access_token: 应用token

    Returns: 表格ID

    """
    tenant_access_token = tenant_access_token or get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data['code'] == 0:
            return [sheet['sheet_id'] for sheet in data['data']['sheets']]
        elif data["code"] == 131006: # 无权限
            raise HTTPException(401, detail="文档未授权,请将文档链接分享范围设置为凡岛")
        else:
            log.error(f"获取表格信息失败: {data['msg']}")
            raise HTTPException(500, detail=data['msg'])
    else:
        log.error(f"获取表格信息失败: {response.text}")
        raise HTTPException(500, detail=response.text)


def get_bitable_id(app_token: str, tenant_access_token: str = None) -> List[str]:
    """
    获取多维表格token
    Args:
        app_token: 多维表格 App 的唯一标识
        tenant_access_token: 应用token

    Returns: 数据表 ID

    """
    tenant_access_token = tenant_access_token or get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data['code'] == 0:
            return [table['table_id'] for table in data['data']['items']]
        elif data["code"] == 131006: # 无权限
            raise HTTPException(401, detail="文档未授权,请将文档链接分享范围设置为凡岛")
        else:
            log.error(f"获取多维表格信息失败: {data['msg']}")
            raise HTTPException(500, detail=data['msg'])


def _create_export_task(payload: dict, tenant_access_token: str = None) -> str:
    """
    调用飞书导出任务接口
    Args:
        payload: 请求体
        tenant_access_token: 应用token

    Returns: 导出任务ID

    """
    tenant_access_token = tenant_access_token or get_tenant_access_token()
    url = "https://open.feishu.cn/open-apis/drive/v1/export_tasks"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    data = response.json()
    if data['code'] == 0:
        return data['data']['ticket']
    elif data["code"] == 1069902: # 无权限
        raise HTTPException(401, detail="文档未授权,请将文档链接分享范围设置为凡岛")
    else:
        log.error(f"创建导出文档任务失败: {data['msg']}")
        raise HTTPException(500, detail=data['msg'])



def create_export_task(doc_token: str, file_type: str, tenant_access_token: str = None) -> List[str]:
    """
    创建导出文档任务
    
    Args:
        doc_token: 文档token
        file_type: 文档类型
        tenant_access_token: 应用token

    Returns: 导出任务ID

    """
    data = {
        "token": doc_token,
        "type": file_type
    }
    result = []
    if file_type in ["docx", "doc"]:
        data["file_extension"] = "docx"
        result.append(_create_export_task(payload=data, tenant_access_token=tenant_access_token))
    elif file_type == "bitable":
        data["file_extension"] = "csv"
        bitable_ids: List[str] = get_bitable_id(doc_token, tenant_access_token)
        log.info(f"bitable_ids: {bitable_ids}")
        for bitable_id in bitable_ids:
            data["sub_id"] = bitable_id
            result.append(_create_export_task(payload=data, tenant_access_token=tenant_access_token))
    elif file_type == "sheet" or file_type == "sheets":
        data["file_extension"] = "csv"
        data["type"] = "sheet"
        sheet_ids: List[str] = get_sheet_token(doc_token, tenant_access_token)
        for sheet_id in sheet_ids:
            data["sub_id"] = sheet_id
            result.append(_create_export_task(payload=data, tenant_access_token=tenant_access_token))
    else:
        log.error(f"不支持的文档类型: {file_type}")
        raise HTTPException(500, detail="不支持的文档类型")
    return result


def get_export_file_token(ticket: str, doc_token: str, tenant_access_token: str = None) -> dict:
    """
    获取导出文档任务状态
    Args:
        ticket: 导出任务ID
        doc_token: 文档token
        tenant_access_token: 应用token

    Returns: 导出信息

    """
    tenant_access_token = tenant_access_token or get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/drive/v1/export_tasks/{ticket}"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    params = {
        "token": doc_token,
    }
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        data: dict = response.json()
        if data['code'] == 0:
            return data['data']
        else:
            log.error(f"获取导出文档任务状态失败: {data['msg']}")
            raise HTTPException(500, detail=data['msg'])
    else:
        log.error(f"获取导出文档任务状态失败: {response.text}")
        raise HTTPException(500, detail=response.text)


def download_file(file_token: str, tenant_access_token: str = None) -> BinaryIO:
    """
    根据导出信息中的file_token下载文件
    Args:
        file_token: 文件token
        tenant_access_token: 应用token
    
    Returns: 二进制文件流
    """
    tenant_access_token = tenant_access_token or get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/drive/v1/export_tasks/file/{file_token}/download"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers) # 返回二进制文件流
    if response.status_code == 200:
        return BytesIO(response.content)
    else:
        log.error(f"下载文件失败: {response.text}")
        raise HTTPException(500, detail=response.text)