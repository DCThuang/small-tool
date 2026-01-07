#!/usr/bin/env python3
import os
import boto3
import tarfile
import shutil
from datetime import datetime, timedelta

# ========== 配置 ==========
S3_BUCKET = "aws-jp-prod-wazuh-s3"
S3_PREFIX = "wazuh-backups/var/ossec"
LOG_BASE = "/var/ossec/logs"
TMP_BASE = "/tmp"

# 日志子目录
LOG_DIRS = [
    "alerts",
    "api",
    "archives",
    "cluster",
    "firewall",
    "wazuh",
]

# 需要打包上传的目录
PACKAGE_DIRS = [
    "/var/ossec/etc",
    "/var/ossec/ruleset",
    "/var/ossec/api",
]

# 初始化 S3 客户端
s3 = boto3.client("s3")

# ========== 获取前一天日期 ==========
def get_yesterday():
    d = datetime.now() - timedelta(days=1)
    return {
        "year": d.strftime("%Y"),
        "month": d.strftime("%b"),
        "day": d.strftime("%d"),
    }

# ========== 上传文件 ==========
def upload_file(local_path, s3_key):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"✅ 上传 {local_path} → s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ 上传失败 {local_path} → {s3_key} : {e}")

# ========== 删除临时文件 ==========
def clean_up(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
            print(f"🗑️ 删除临时文件 {path}")
        elif os.path.isdir(path):
            shutil.rmtree(path)
            print(f"🗑️ 删除临时目录 {path}")

# ========== 备份日志（只上传前一天） ==========
def backup_logs():
    date = get_yesterday()
    year, month, day = date["year"], date["month"], date["day"]

    for log_dir in LOG_DIRS:
        local_dir = os.path.join(LOG_BASE, log_dir)
        if not os.path.isdir(local_dir):
            print(f"⚠️ 跳过不存在目录: {local_dir}")
            continue

        print(f"\n📁 处理目录: {local_dir}")

        for root, _, files in os.walk(local_dir):
            # 判断路径中是否包含前一天年月
            if f"{year}" not in root or f"{month}" not in root:
                continue

            for filename in files:
                # 文件名中必须包含前一天日
                if f"-{day}." not in filename and f"-{day}-" not in filename:
                    continue

                local_file = os.path.join(root, filename)
                rel_path = os.path.relpath(local_file, "/var/ossec")
                s3_key = f"{S3_PREFIX}/{rel_path}"
                upload_file(local_file, s3_key)

# ========== 通用打包上传函数 ==========
def backup_dir(dir_path):
    if not os.path.isdir(dir_path):
        print(f"⚠️ {dir_path} 不存在，跳过")
        return

    date = get_yesterday()
    archive_name = f"{os.path.basename(dir_path)}-backup-{date['year']}-{date['month']}-{date['day']}.tar.gz"
    archive_path = os.path.join(TMP_BASE, archive_name)

    print(f"\n📦 打包 {dir_path} → {archive_path}")
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(dir_path, arcname=os.path.basename(dir_path))

    s3_key = f"{S3_PREFIX}/{os.path.basename(dir_path)}/{date['year']}/{date['month']}/{date['day']}/{archive_name}"
    upload_file(archive_path, s3_key)
    clean_up(archive_path)

# ========== 执行 ==========
if __name__ == "__main__":
    print("🚀 开始 Wazuh 前一天增量日志 + etc/ruleset/api 备份")
    backup_logs()

    for d in PACKAGE_DIRS:
        backup_dir(d)

    print("🎉 备份完成")
