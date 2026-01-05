import os
import subprocess
import shutil
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# ---------- S3 上传 ----------
def upload_to_s3(file_path, bucket_name, s3_prefix="backups/"):
    if not os.path.isfile(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return False

    file_name = os.path.basename(file_path)
    s3_key = os.path.join(s3_prefix, file_name).replace("\\", "/")

    try:
        s3 = boto3.client("s3")
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ 上传成功: s3://{bucket_name}/{s3_key}")
        return True
    except (ClientError, BotoCoreError) as e:
        print(f"❌ 上传失败: {e}")
        return False


# ---------- 清理本地文件/目录 ----------
def clean_up(*paths):
    for path in paths:
        try:
            if os.path.isfile(path):
                os.remove(path)
                print(f"🗑️ 删除文件: {path}")
            elif os.path.isdir(path):
                shutil.rmtree(path)
                print(f"🗑️ 删除目录: {path}")
        except Exception as e:
            print(f"⚠️ 清理失败 {path}: {e}")


# ---------- 备份任意文件夹 ----------
def backup_folder(backup_dir, source_dir, exclude_files=None):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    folder_name = os.path.basename(source_dir.rstrip("/"))
    archive_name = f"{folder_name}_backup_{timestamp}.tar.gz"
    archive_path = os.path.join(backup_dir, archive_name)

    if not os.path.isdir(source_dir):
        print(f"❌ 源目录不存在: {source_dir}")
        return None

    print(f"🔔 开始备份目录: {source_dir}")

    cmd = [
        "tar", "czf", archive_path,
        "-C", os.path.dirname(source_dir),
        os.path.basename(source_dir),
        "--ignore-failed-read",
        "--warning=no-file-changed"
    ]

    if exclude_files:
        for f in exclude_files:
            cmd.insert(3, f"--exclude={f}")

    print(f"执行命令: {' '.join(cmd)}")

    if subprocess.call(cmd) != 0:
        print(f"⚠️ 目录 {source_dir} 备份完成，但有文件变化警告")
    else:
        print(f"✅ 目录备份完成: {archive_path}")

    return archive_path


# ---------- 备份 MongoDB 所有可访问数据库 ----------
def backup_all_mongo(backup_dir, mongo_user, mongo_pass, auth_db="admin", host="localhost", port=27017):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dump_dir = os.path.join(backup_dir, f"mongo_dump_{timestamp}")

    mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{host}:{port}/{auth_db}"
    client = MongoClient(mongo_uri)

    try:
        db_names = client.list_database_names()
        print(f"✅ 可访问 MongoDB 数据库: {db_names}")
    except OperationFailure as e:
        print(f"❌ 无法列出数据库: {e}")
        return None

    for db_name in db_names:
        db_dump_dir = os.path.join(dump_dir, db_name)
        os.makedirs(db_dump_dir, exist_ok=True)

        cmd = [
            "mongodump",
            "--username", mongo_user,
            "--password", mongo_pass,
            "--authenticationDatabase", auth_db,
            "--db", db_name,
            "--out", db_dump_dir
        ]

        print(f"🔔 开始备份 MongoDB: {db_name}")
        print(f"执行命令: {' '.join(cmd)}")

        if subprocess.call(cmd) != 0:
            print(f"❌ MongoDB {db_name} 备份失败")
        else:
            print(f"✅ MongoDB {db_name} 备份完成")

    archive_path = dump_dir + ".tar.gz"
    cmd_tar = [
        "tar", "czf", archive_path,
        "-C", backup_dir,
        os.path.basename(dump_dir),
        "--warning=no-file-changed"
    ]

    print(f"🔔 打包 MongoDB: {' '.join(cmd_tar)}")

    if subprocess.call(cmd_tar) != 0:
        print("❌ MongoDB 打包失败")
        return None

    clean_up(dump_dir)
    print(f"✅ MongoDB 备份完成: {archive_path}")

    return archive_path


# ---------- 主流程 ----------
if __name__ == "__main__":
    s3_bucket = "aws-jp-prod-wazuh-s3"
    backup_dir = os.path.expanduser(f"~/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(backup_dir, exist_ok=True)

    # ---------- n8n ----------
    n8n_data_dir = "/data/n8n_data"
    n8n_archive = backup_folder(
        backup_dir,
        n8n_data_dir,
        exclude_files=["*.log", "*.cache", "n8nEventLog.log"]
    )

    if n8n_archive and upload_to_s3(n8n_archive, s3_bucket, "n8n_backups/"):
        clean_up(n8n_archive)

    # ---------- MongoDB ----------
    mongo_user = "admin"
    mongo_pass = "xxxxxx"
    mongo_archive = backup_all_mongo(backup_dir, mongo_user, mongo_pass)

    if mongo_archive and upload_to_s3(mongo_archive, s3_bucket, "mongo_backups/"):
        clean_up(mongo_archive)

    # ---------- 最外层目录清理（关键新增） ----------
    try:
        if os.path.isdir(backup_dir) and not os.listdir(backup_dir):
            shutil.rmtree(backup_dir)
            print(f"🗑️ 删除空的备份目录: {backup_dir}")
        else:
            print(f"ℹ️ 备份目录未清空，保留: {backup_dir}")
    except Exception as e:
        print(f"⚠️ 删除备份目录失败: {e}")

    print("🎉 全部备份流程完成")
